from unittest.case import TestCase
import unittest
import time
import ooi.timer
from math import fabs
class TestTimer(TestCase):

    def test_use_case_example(self):
        a = ooi.timer.Accumulator(keys=['half','done', 'fish' ])
        t = ooi.timer.Timer()
        time.sleep(0.05)
        t.complete_step('half')
        time.sleep(0.04)
        t.complete_step('done')
        a.add(t)
        a.add_value('fish', 3)

        t = ooi.timer.Timer()
        time.sleep(0.06)
        t.complete_step('half')
        time.sleep(0.05)
        t.complete_step('done')
        a.add(t)
        a.add_value('fish', 3.5)
        a.add_value('fish', 3.5)
        a.add_value('fish', 3.5)

        a.log()
        print str(a)
        # self.assertNothingCrashed()

if __name__ == '__main__':
    unittest.main()
