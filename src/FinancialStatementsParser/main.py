from typing import Dict

import re
from datetime import datetime
import pandas as pd
import pypdfium2 as pdfium
import jieba


JIEBA_USERDICT = "financial_statements.txt"


def init_jieba() -> None:
    jieba.load_userdict(JIEBA_USERDICT)


class FinancialStatementParser:
    QUATER_MAP = {
        "1月1至3月31": "Q1",
        "4月1至6月30": "Q2",
        "7月1至9月30": "Q3",
        "10月1至12月31": "Q4",
        "1月1至6月30": "H1",
        "7月1至12月31": "H2",
    }

    month_pattern = r"[0-9]{1,2}[月\.][0-9]{1,2}"
    date_pattern = r"[0-9]{3}[年\.]" + month_pattern
    quarter_pattern = date_pattern + "[日至~]{0,}" + month_pattern
    PERIOD_PATTERN = (
        r"[0-9]{3}[年\.][0-9]{1,2}[月\.][0-9]{1,2}(?:[日至~]{0,}[0-9]{1,2}[月\.][0-9]{1,2})?"
    )
    # PERIOD_PATTERN =  f'{quarter_pattern}|{date_pattern}'

    SUBJECT_PATTERN = r"\d{1,3}(?:,\d{3})*\){0,1}|-"

    def __init__(self, pdf_path: str):
        init_jieba()
        self.pdf = pdfium.PdfDocument(pdf_path)
        self.data = []
        self.use_dates = []
        self.duplicated_accounts = ["成本", "其他", "合計"]
        self.unwanted_account = ["合計"]
        self.subjects_dict = self._load_subjects_dict()

    def get_pandas_df(self):
        df = pd.DataFrame(self.data, columns=["Account", "Subject", "Period", "Value"])

        return self._formmater(df)

    @staticmethod
    def _formmater(df):
        return df

    def parse(self, start_range: int, end_range: int):
        for page_idx in range(start_range, end_range + 1):
            print(f"page------{page_idx}")
            self._parse_page(page_idx)

    def _parse_page(self, page_idx: int):
        paragraphs = self._get_paragraphs_from_page(self.pdf[page_idx])
        self.set_page_params()

        for sentence in paragraphs:
            print("s---", sentence)
            cleaned_sentence = self._clean_sentence(sentence)
            if "月" in sentence:
                date, cleaned_sentence = self._extract_date(cleaned_sentence)
                print(date, cleaned_sentence)
                self.use_dates.extend(date)

            tokens = list(jieba.cut(cleaned_sentence))
            print("tokens---", tokens)
            if self._check_multiple_subject_cols(tokens) and tokens:
                self.fourth_grade_account = tokens
            print(self.fourth_grade_account)
            self._process_tokens(
                tokens,
                cleaned_sentence,
            )

    def _process_tokens(self, tokens, cleaned_sentence):
        for token in tokens:
            token = self.change_token(token)
            if self.subjects_dict.get(token) and re.search(r"\d", cleaned_sentence):
                self.subjects_dict[token]["Count"] += 1
                nums = self.extract_subject_nums(cleaned_sentence)
                self._append_data(token, nums)

    def _load_subjects_dict(self) -> Dict[str, Dict[str, int]]:
        """Get all the subject mapping we need"""

        with open("financial_statements.txt", encoding="utf-8") as f:
            subjects = f.read().splitlines()

        # Add this to let needed tokens in a sentence correspond
        # to all the subject
        subject_d = {subject: 0 for subject in self.duplicated_accounts}
        main_subject = ""
        for text in subjects:
            if not text:
                continue

            if text.startswith("#"):
                main_subject = text[2:]
                continue

            subject = f"{main_subject}-{text}" if text == "其他" else text
            subject_d[subject] = {"Count": 0, "Period": [], "Subject": main_subject}

        return subject_d

    def _get_subject_details_pages(self, pdf):
        description = r"(?<=重要會計項目之說明).*\d{1,}.*\d{1,}"
        for page in pdf:
            content = page.get_textpage().get_text_range()
            res = re.findall(description, content)
            if len(res) != 0:
                matches = re.findall(r"\d{1,}", res[0])

                return int(matches[0]) - 1, int(matches[1]) - 1  # offset for pdf pages

        raise ValueError("No table of contents found.")

    def set_page_params(self):
        self.other_count = 0
        self.fourth_grade_account = None
        self.use_dates = []
        self.last_append_date_idx = 0
        self.subjects_dict["成本"]["Period"] = []

    @staticmethod
    def _clean_sentence(sentence: str) -> str:
        return re.sub(r" |\r|日", "", sentence)

    @staticmethod
    def remove_spaces_between_words(sentence):
        return re.sub(r"[\u3000 -]+([\u4e00-\u9fff]+)[\u3000 ]{0,}", r"\1", sentence)

    def _extract_date(self, sentence):
        """Return dates and removed dates string from sentence"""
        dates_str = set(re.findall(self.PERIOD_PATTERN, sentence))
        removed_date_sentence = self.remove_dates(dates_str, sentence)
        if not self._is_complete_date(
            dates_str, sentence
        ) and not self._check_multiple_subject_cols(removed_date_sentence):
            return [], removed_date_sentence

        return (
            sorted([self.transform_date(date) for date in dates_str], reverse=True),
            removed_date_sentence,
        )

    @staticmethod
    def remove_dates(dates_str, sentence):
        for date in dates_str:
            sentence = re.sub(date, "", sentence)
        return sentence

    @staticmethod
    def _is_complete_date(dates_str, sentence) -> bool:
        return sum(len(s) for s in dates_str) == len(sentence)

    def transform_date(self, s: str):
        # Transorm date to D.C.
        year = int(s[:3]) + 1911

        if not re.search("至|~", s):
            remove_year_month_s = re.sub("年|月", "-", s)
            date_str = str(year) + remove_year_month_s[3:]
            return datetime.strptime(date_str, "%Y-%m-%d")
        return str(year) + "-" + self.QUATER_MAP.get(s[4:])

    def _get_paragraphs_from_page(self, page):
        paragraphs = page.get_textpage().get_text_range()
        return self.remove_spaces_between_words(paragraphs).split("\n")

    def extract_subject_nums(self, sentence):
        matches = re.findall(self.SUBJECT_PATTERN, sentence)
        nums = []
        for num in matches:
            cleaned_num = num.replace(",", "")
            if ")" in cleaned_num:
                nums.append(-int(cleaned_num.replace(")", "")))
            elif num == "-":
                nums.append(0)
            else:
                nums.append(int(cleaned_num))
        return nums

    def _check_multiple_subject_cols(self, tokens):
        if isinstance(tokens, str):
            tokens = jieba.cut(tokens)
        return all(token in self.subjects_dict for token in tokens)

    def change_token(self, token):
        # Use for other account, since this account exists in every subject
        if token != "其他":
            return token

        elif not self.data:
            return token

        last_account = self.data[-1][0]
        if not last_account.endswith("其他|合計"):
            last_subject = self._reset_other_count(last_account)
            token = last_subject + "-" + token
            self.other_count -= 1
        elif self.other_count > 0:
            token = last_account
            self.other_count -= 1
        return token

    def _reset_other_count(self, last_account: str):
        """Verify how many other account I should append into data"""
        last_account_d = self.subjects_dict.get(last_account)
        self.other_count = last_account_d["Count"]
        return last_account_d["Subject"]

    def _append_data(self, token, nums):
        subject_period = self.subjects_dict[token]["Period"]
        use_dates = [date for date in self.use_dates if not date in subject_period]
        # use_dates = self.use_dates
        print("all_dates", use_dates)
        print("tokens", token)
        print(self.fourth_grade_account)

        if not self.fourth_grade_account or len(set(self.fourth_grade_account)) == 1:
            account = self.remove_duplicated_account(self.fourth_grade_account)

            for date, num in zip(use_dates, nums):
                print(date, token)
                subject_period.append(date)
                self.data.append([token, account, date, num])
        else:
            use_date = use_dates[0]
            subject_period.append(use_date)
            print(token, use_date)
            for num, account in zip(nums, self.fourth_grade_account):
                print([token, account, use_date, num])
                self.data.append([token, account, use_date, num])

    @staticmethod
    def remove_duplicated_account(fourth_grade_account):
        if fourth_grade_account:
            return fourth_grade_account[0]
        return None
