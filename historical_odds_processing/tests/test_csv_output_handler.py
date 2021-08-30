import numpy as np
import os
import pandas as pd

from unittest import TestCase

from historical_odds_processing.store.db_creation.csv_output_handler import CSVOutputHandler


class TestCSVOutputHandler(TestCase):
    def __init__(self, methodName="runTest"):
        super(TestCSVOutputHandler, self).__init__(methodName=methodName)
        self.testFilename = "testFile.csv"
        self.firstVariableName = "firstVar"
        self.secondVariableName = "secondVar"
        self.validTest1 = {self.firstVariableName: 100, self.secondVariableName: "second"}
        self.validTest2 = {self.firstVariableName: 200, self.secondVariableName: "second2"}
        self.validTest3 = {self.firstVariableName: 300}
        self.invalidTest = {"randomName": "stuff", "randomName2": 1000}

    def setUp(self):
        super().setUp()
        self.csvOutputHandler = CSVOutputHandler(
            fileName=self.testFilename, tableFields=(self.firstVariableName, self.secondVariableName)
        )

    def tearDown(self):
        super().tearDown()
        os.remove(self.testFilename)

    def test_csv_output_handler_valid(self):
        self.csvOutputHandler.add(data=self.validTest1)
        self.csvOutputHandler.add(data=self.validTest2)
        self.csvOutputHandler.add(data=self.validTest3)
        self.csvOutputHandler.close()
        savedData = pd.read_csv(filepath_or_buffer=self.testFilename)
        expectedFirstVarData = [
            self.validTest1[self.firstVariableName],
            self.validTest2[self.firstVariableName],
            self.validTest3[self.firstVariableName],
        ]
        self.assertSequenceEqual(seq1=list(savedData[self.firstVariableName].values), seq2=expectedFirstVarData)
        expectedSecondVarData = [self.validTest1[self.secondVariableName], self.validTest2[self.secondVariableName], np.nan]
        self.assertSequenceEqual(seq1=list(savedData[self.secondVariableName].values), seq2=expectedSecondVarData)

    def test_csv_output_handler_invalid(self):
        self.assertRaises(ValueError, self.csvOutputHandler.add, self.invalidTest)
