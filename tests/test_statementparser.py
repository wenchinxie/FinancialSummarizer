from financial_statement_parser.parser import (
    FinancialStatementParser,
    get_statement_content,
)


class TestFinancialstatementParser:
    statement_file_path = r"C:\Users\s3309\Desktop\Investment\quarter_reports\to_parse\202201_1326_AI1_20221016_130222.txt"
    statement_content = get_statement_content(statement_file_path)

    parser = FinancialStatementParser(statement_content)

    def test_parse(self):
        self.parser.parse()

        assert 1 == 1

    def test_parse_df(self):
        try:
            self.parser.parse()
        except Exception as e:
            print(e)
            df = self.parser.get_pandas_df()
            print(df)

        assert 1 == 1

    def test_merge_subject(self):
        s1 = "                         土地及                                           運輸設備         未完工程"
        s2 = "                        土地改良物      房屋及建築              機器設備             及其他設備        及待驗設備            合計"

        merged_subjects = FinancialStatementParser.merge_subjects(s1, s2)
        assert merged_subjects[0] == "土地及土地改良物"
        assert merged_subjects[1] == "房屋及建築"
        assert merged_subjects[2] == "機器設備"
        assert merged_subjects[3] == "運輸設備及其他設備"
        assert merged_subjects[4] == "未完工程及待驗設備"
        assert merged_subjects[5] == "合計"
