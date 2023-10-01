import functools
import os
import re
from datetime import datetime
from typing import Dict, List

import jieba
import pandas as pd

cur_dir = os.path.dirname(__file__)
JIEBA_USERDICT = os.path.join(cur_dir, "financial_statements.txt")

QUATER_MAP = {
    "1月1至3月31": "Q1",
    "4月1至6月30": "Q2",
    "7月1至9月30": "Q3",
    "10月1至12月31": "Q4",
    "1月1至6月30": "H1",
    "7月1至12月31": "H2",
}


def get_statement_content(statement_file_path: str):
    with open(statement_file_path, encoding="UTF-8") as f:
        return f.read().splitlines()


PERIOD_PATTERN = (
    r"[0-9]{3}[年\.][0-9]{1,2}[月\.][0-9]{1,2}(?:[日至~]{0,}[0-9]{1,2}[月\.][0-9]{1,2})?"
)
BREAK_DATE_PATTERN = r"^至\d+月\d+"

NUM_PATTERN = r"\d{1,3}(?:,\d{3})*\){0,1}|-"
PERCENT_PATTERN = r"\d+\.\d+%~\d+\.\d+%"
SUBJECT_PATTERN = r"\([一二三四五六七八九十]+\)"
REFERRED_ACCOUNTS = {"其他", "合計", "匯率影響數", "期初餘額", "期末餘額", "利率區間"}
DUPLICATED_CONTENT = {"成本"}.union(REFERRED_ACCOUNTS)


def init_jieba() -> None:
    print(JIEBA_USERDICT)
    jieba.load_userdict(JIEBA_USERDICT)


def load_subjects_dict() -> Dict[str, Dict[str, int]]:
    """Get all the subject mapping we need"""

    with open(JIEBA_USERDICT, encoding="utf-8") as f:
        subjects = f.read().splitlines()

    # Add this to let needed tokens in a sentence correspond
    # to all the subject
    subject_d = {subject: {"Count": 0} for subject in DUPLICATED_CONTENT}
    subject_d["main subjects"] = {}
    main_subject = ""
    for text in subjects:
        if not text:
            continue

        if text.startswith("#"):
            main_subject = text[2:]
            subject_d["main subjects"][main_subject] = {"Remark": ""}
            continue

        subject = f"{main_subject}-{text}" if text in REFERRED_ACCOUNTS else text
        subject_d[subject] = {"Count": 0, "Period": [], "Subject": main_subject}

    return subject_d


class FinancialStatementParser:
    referred_accounts = REFERRED_ACCOUNTS
    duplicated_accounts = DUPLICATED_CONTENT
    NUM_PATTERN = NUM_PATTERN
    PERIOD_PATTERN = PERIOD_PATTERN
    PERCENT_PATTERN = PERCENT_PATTERN
    CHINESE_ANNOTATION_PATTERN = r"\([\u4e00-\u9fff]+\)"
    QUATER_MAP = QUATER_MAP

    def __init__(self, content: List[str]):
        init_jieba()
        self.content = content
        self.access_import_subjects = False
        self.data = []
        self.use_dates = []
        self.unwanted_account = ["合計"]
        self.subjects_dict = load_subjects_dict()
        self.unwanted_words = [" "]
        self.category = None
        self._next_sentence_remove = ""
        self.continued_words = ""
        self.last_sentence = ""
        self.combining_words = ["認列收入"]
        self.fourth_grade_account = []
        self._sentence = None
        self._dates_locs = []
        self.other_count = 0
        self.last_append_date_idx = 0
        self.subject = ""
        self._subject_annotations = ""
        self._remark = ""
        self._wanted_words = ["費損之存貨成本"]
        self._cur_subject = ""
        self._tokens = []

    def get_pandas_df(self):
        df = pd.DataFrame(
            self.data,
            columns=[
                "Account",
                "4-th Account",
                "Period",
                "Value",
                "Category",
                "Subject Annotation",
                "Subject",
            ],
        )
        return df

    def parse(self):
        for i, sentence in enumerate(self.content):
            if self._is_sentence_skippable(i, sentence):
                print("Skip this sentence----")
                continue

            if "月" in sentence:
                self._reform_sentence_and_extract_dates_str_and_locs()

            # Extract subject-related annotations
            self._extract_subject_annotations()

            tokens = jieba.lcut(self._sentence)
            if self._are_all_tokens_subjects(tokens) and tokens:
                if self._is_category_token(tokens):
                    self.category = tokens[0]
                else:
                    self.use_dates = [self.use_dates[-1]]
                    self.fourth_grade_account = tokens

            self._process_tokens(tokens)

    def _is_sentence_skippable(self, i, sentence):
        # Ignore all strings until reach to important subjects regions
        # In case the programming misunderstands
        if not self.access_import_subjects:
            self._does_reach_important_accounts(sentence)
            return True

        # self.initialize_params_if_new_subject(sentence)

        print("cur_dates", self.use_dates)
        print("length-----", len(sentence))
        print("s---", sentence)

        self._sentence = self._reform_sentence(i)
        len_orig_sentence = len(sentence)
        len_cleaned_sentence = len(self._sentence)
        clean_percentage = len_cleaned_sentence / (len_orig_sentence + 1)
        print(f"reformed----{self._sentence}!")
        print(
            f"clean percentage:{clean_percentage}",
        )

        if self._is_new_subject():
            self._initialize_params_if_new_subject()
            return True

        print("remark---", self._remark)
        if self._is_remark():
            self._record_remark()
            return True

        if self._remark:
            self._remark += self._sentence
            return True

        # If the paragraph is short and is in subject dict, then it might be an account
        if len_orig_sentence <= 15:
            if "月" in sentence:
                self._reform_sentence_and_extract_dates_str_and_locs()
                return True

            if self.subjects_dict.get(self._sentence):
                self.fourth_grade_account.append(self._sentence)
            else:
                self.last_sentence += self._sentence
            return True

        # If the subjects are too long to stay in the same line
        # See test_merge_subject
        # Skip if we found the fourth-grade accounts since it might be over the length and cleaned ratio
        if (
            len_orig_sentence >= 60
            and clean_percentage <= 0.3
            and not self.fourth_grade_account
        ):
            are_all_tokens_subjects = self._are_all_tokens_subjects(sentence)

            # In case of 帳面金額 帳面金額 帳面金額
            if are_all_tokens_subjects and len(self._tokens) != len(set(self._tokens)):
                self.fourth_grade_account = self._tokens
                return True
            if self._are_all_tokens_subjects(sentence):
                return False
            if self.last_sentence:
                # TODO: remove date
                self.fourth_grade_account = self.merge_subjects(
                    self.last_sentence, sentence
                )
                return True
            self.last_sentence = sentence
            return True

        return False

    def _process_tokens(self, tokens):
        print(tokens)
        print("cleaned_sentence", self._sentence)

        for token in tokens:
            token = self.change_token(token)
            print("token get?", self.subjects_dict.get(token, "No token"))
            if self.subjects_dict.get(token) and re.search(r"\d", self._sentence):
                print("append data-------------------------------")
                self._reset_fourth_grade_account_and_category(token)
                self.subjects_dict[token]["Count"] += 1
                nums = self.extract_subject_nums()
                self._append_data(token, nums)

    def _append_data(self, token, nums):
        subject_period = self.subjects_dict[token]["Period"]
        use_dates = self._find_usable_dates(token)
        print(self.fourth_grade_account)
        print("nums----", nums)

        if not self.fourth_grade_account or len(set(self.fourth_grade_account)) == 1:
            assert len(use_dates) == len(nums), "Not matched dates and nums"

            account = self.remove_duplicated_account(self.fourth_grade_account)

            for date, num in zip(use_dates, nums):
                print(date, token, num)
                subject_period.append(date)
                self.data.append(
                    [
                        token,
                        account,
                        date,
                        num,
                        self.category,
                        self._subject_annotations,
                        self._cur_subject,
                    ]
                )
        else:
            use_date = use_dates[0]
            subject_period.append(use_date)
            print(token, use_date)
            for num, account in zip(nums, self.fourth_grade_account):
                print([token, account, use_date, num])
                self.data.append(
                    [
                        token,
                        account,
                        use_date,
                        num,
                        self.category,
                        self._subject_annotations,
                        self._cur_subject,
                    ]
                )

    def _reform_sentence(self, i):
        last_cleaned_sentence = (
            self._clean_sentence(self.content[i - 1]) if i != 0 else ""
        )
        cur_cleaned_sentence = self._clean_sentence(self.content[i])

        if re.search("|".join(self.combining_words), cur_cleaned_sentence):
            self.last_sentence += cur_cleaned_sentence
            return cur_cleaned_sentence

        if re.search(BREAK_DATE_PATTERN, self.content[i]):
            return last_cleaned_sentence + cur_cleaned_sentence

        tokens = jieba.cut(cur_cleaned_sentence)

        if any(token in self.continued_words for token in tokens):
            return last_cleaned_sentence + cur_cleaned_sentence

        return cur_cleaned_sentence

    def _extract_subject_annotations(self):
        # initialize subject annotations for each sentence
        if self._subject_annotations:
            self._subject_annotations = ""

        if not self._sentence:
            return

        match = re.search(self.CHINESE_ANNOTATION_PATTERN, self._sentence)
        if match:
            subject_annotations = match.group(0)
            self._subject_annotations = subject_annotations
            self._sentence = re.sub(subject_annotations, "", self._sentence)

    def remove_token(self):
        sentence = self._sentence
        tokens = jieba.cut(sentence)

        for token in tokens:
            if self.subjects_dict.get(token):
                sentence = re.sub(token, "", sentence)

        return sentence

    def _is_category_token(self, tokens):
        """If the length of use_dates in a subject is larger than
        length of tokens, then it is category. However, the length of use_dates
        will decrease along with the data appending, it might be fourth grade account
        when the length of both are equal.
        In this case, return False if there is fourth_grade_account already,
        and reset both when the appended data subject is not consistent with last one
        """
        token_use_dates = self._find_usable_dates(tokens[0])
        if not self.fourth_grade_account:
            return len(token_use_dates) > len(tokens)
        return False

    def _add_date_consumed(self, next_sentence):
        cleaned_next_sentence = self._clean_sentence(next_sentence)
        match = re.search(self.BREAK_DATE_PATTERN, cleaned_next_sentence)
        if match:
            consume_text = match.group(0)
            self._next_sentence_remove = consume_text
            return consume_text
        return ""

    def _is_new_subject(self):
        # remove nums in chinese, such as: (一)
        removed_nums_sentence = re.sub(SUBJECT_PATTERN, "", self._sentence)
        print("removed sentence", removed_nums_sentence)
        print("exists?", self.subjects_dict["main subjects"].get(removed_nums_sentence))
        if removed_nums_sentence in self.subjects_dict["main subjects"]:
            print("True and new subject")
            self._cur_subject = removed_nums_sentence
            return True
        return False

    def _is_remark(self):
        """Record the remark for each subject, however the exception is like below:
        1. 本集團當期認列為費損之存貨成本...
        :return: bool
        """
        if re.search("^\d\.|\(\d\.{,1}\)", self._sentence):
            return True
        return False

    def _record_remark(self):
        if re.search("|".join(self._wanted_words), self._sentence):
            return None

        if not self._remark:
            self._remark = self._sentence
        else:
            print(self.data[-1][0])
            self.subjects_dict["main subjects"][self._cur_subject][
                "Remark"
            ] += self._remark
            self._remark = ""

    def _initialize_params_if_new_subject(self):
        self.other_count = 0
        self.fourth_grade_account = []
        self.use_dates = []
        self.last_append_date_idx = 0
        self.category = None
        self.last_sentence = ""
        self._remark = ""

        for subject in self.duplicated_accounts:
            self.subjects_dict[subject]["Period"] = []

    @functools.lru_cache
    def _clean_sentence(self, sentence: str) -> str:
        unwanted_pattern = r" |\r|日|－|\(註\d{0,}\)|^0|\d+年度|、"
        if self._next_sentence_remove:
            unwanted_pattern += self._next_sentence_remove
            self._next_sentence_remove = ""

        return re.sub(unwanted_pattern, "", sentence)

    def remove_unwanted_words_between_chinese(self, sentence):
        unwanted_words = "".join(self.unwanted_words)
        return re.sub(
            rf"[\u3000{unwanted_words}]+([\u4e00-\u9fff]+)[\u3000 ]{0,}",
            r"\1",
            sentence,
        )

    def _reform_sentence_and_extract_dates_str_and_locs(self):
        """Return dates and removed dates string from sentence"""

        dates_str = self._extract_dates_and_record_locs()

        # For the case: 111年8月1日 土地 房地產 設備
        # Except the date, all the tokens are what we need
        self._remove_date_str_from_sentence(dates_str)

        # TODO: If the situation is date, account,account,account
        # TODO: add is_statement to avoid all paragraphs
        # like 111年6月1日 土地 房地產 待驗設備, then set it as use date

        if not self._are_only_dates_in_sentence(
            dates_str
        ) and not self._are_all_tokens_subjects(self._sentence):
            pass

        transformed_dates_str = sorted(
            [self.transform_date(date) for date in dates_str], reverse=True
        )
        self.use_dates.extend(transformed_dates_str)

    def _extract_dates_and_record_locs(self):
        matches = re.finditer(self.PERIOD_PATTERN, self._sentence)

        # record locs
        dates_str = []
        for match in matches:
            self._dates_locs.append(match.span())
            dates_str.append(match.group())

        return dates_str

    def _remove_date_str_from_sentence(self, dates_str):
        for date in dates_str:
            self._sentence = re.sub(date, "", self._sentence)

    def _are_only_dates_in_sentence(self, dates_str) -> bool:
        return sum(len(s) for s in dates_str) == len(self._sentence)

    def transform_date(self, s: str):
        # Transorm date str to D.C.
        year = int(s[:3]) + 1911

        if not re.search("至|~", s):
            remove_year_month_s = re.sub("年|月", "-", s)
            date_str = str(year) + remove_year_month_s[3:]
            return datetime.strptime(date_str, "%Y-%m-%d")
        return str(year) + "-" + self.QUATER_MAP.get(s[4:])

    def _get_paragraphs_from_page(self, page):
        paragraphs = page.get_textpage().get_text_range()
        return self.remove_unwanted_words_between_chinese(paragraphs).split("\n")

    def extract_subject_nums(self):
        sentence = self.remove_token()
        nums = []

        if re.search("%", sentence):
            matches = re.findall(self.PERCENT_PATTERN, sentence)
            nums.extend(matches)

        else:
            matches = re.findall(self.NUM_PATTERN, sentence)

            for num in matches:
                cleaned_num = num.replace(",", "")
                if ")" in cleaned_num:
                    remove_right_bucket_str = cleaned_num.replace(")", "")
                    nums.append(-int(remove_right_bucket_str))
                elif num == "-":
                    nums.append(0)
                else:
                    nums.append(int(cleaned_num))
        return nums

    def _are_all_tokens_subjects(self, tokens, main_subjects=False):
        if isinstance(tokens, str):
            remmoved_space_sentence = re.sub(" ", "", tokens)
            tokens = jieba.lcut(remmoved_space_sentence)

        self._tokens = tokens
        if main_subjects:
            return all(
                self.subjects_dict["main subjects"].get(token) for token in tokens
            )
        return all(token in self.subjects_dict for token in tokens)

    def change_token(self, token):
        # Use for other account, since this account exists in every subject
        if token not in self.referred_accounts or not self.data:
            return token

        last_account = self.data[-1][0]
        print("count---", self.other_count)
        print("cur-token", token)
        if not re.search(rf"[{''.join(self.referred_accounts)}]$", last_account):
            last_subject = self._reset_other_count(last_account)
            token = last_subject + "-" + token
            print("last subject+")
            self.other_count -= 1
        elif self.other_count > 0:
            print(self.other_count, last_account, token)
            token = last_account if last_account.split("-")[1] == token else token
            self.other_count -= 1
        return token

    def _reset_other_count(self, last_account: str):
        """Verify how many other account I should append into data"""
        last_account_d = self.subjects_dict.get(last_account)
        self.other_count = last_account_d["Count"]
        return last_account_d["Subject"]

    def _find_usable_dates(self, token: str):
        return [
            date
            for date in self.use_dates
            if not date in self.subjects_dict[token]["Period"]
        ]

    def _reset_fourth_grade_account_and_category(self, token: str):
        if not self.data or not (self.category and self.fourth_grade_account):
            return

        last_account = self.data[-1][0]
        if (
            not self.subjects_dict[last_account]["Subject"]
            == self.subjects_dict[token]["Subject"]
        ):
            self.fourth_grade_account = None
            self.category = None

    @staticmethod
    def remove_duplicated_account(fourth_grade_account):
        if fourth_grade_account:
            return fourth_grade_account[0]
        return None

    def _does_reach_important_accounts(self, sentence):
        cleaned_sentence = self._clean_sentence(sentence)
        if re.search("重要會計項目之說明", cleaned_sentence):
            self.access_import_subjects = True

    @staticmethod
    def merge_subjects(s1, s2):
        merged_subjects = []
        subject_s1 = ""
        subject_s2 = ""

        for i, n in enumerate(s2):
            if i > len(s1) - 1:
                pass
            elif s1[i] != " ":
                subject_s1 += s1[i]

            if n != " ":
                subject_s2 += n
            elif n == " " and subject_s2:
                subject_s2 = subject_s1 + subject_s2
                merged_subjects.append(subject_s2)
                subject_s2 = ""
                subject_s1 = ""

        if subject_s2:
            merged_subjects.append(subject_s2)
        return merged_subjects
