from unittest.case import TestCase
import unittest
import time
import ooi.timer
from math import fabs
class TestTimer(TestCase):
    def setUp(self):
        self.op1_times = iter([ .01, .02 ])
        self.a1 = ooi.timer.Accumulator()

        self.op2_step1_times = iter([ .005, .015, .005, .005])
        self.op2_step2_times = iter([ .01, .02, .01, .01])
        self.a2 = ooi.timer.Accumulator()

    def test_found_caller(self):
        import importable.create_timer
        t = importable.create_timer.t
        self.assertEquals('timing.importable.create_timer', t.logger.name)

    def test_time_event(self):
        t = ooi.timer.Timer()

        time.sleep(0.01)
        t.complete_step('pause')

        time.sleep(0.02)
        t.complete_step()

        self.assertEquals(3, len(t.times))

    def one_step_operation(self):
        t = ooi.timer.Timer()
        time.sleep(self.op1_times.next())
        t.complete_step()
        self.a1.add(t)

    def test_stats_one_step(self):
        try:
            while True:
                self.one_step_operation()
        except StopIteration:
            pass

        self.assertEquals(2, self.a1.get_count())
        self.assertAlmostEqual(self.a1.get_average(), 0.015, places=3 )
        self.assertTrue( fabs(self.a1.get_average()-0.015)<.001 )
        self.assertEquals(0, self.a1.get_standard_deviation())


    def two_step_operation(self):
        t = ooi.timer.Timer()
        time.sleep(self.op2_step1_times.next())
        t.complete_step('one')
        time.sleep(self.op2_step2_times.next())
        t.complete_step('two')
        self.a2.add(t)

    def test_stats_two_steps(self):
        try:
            while True:
                self.two_step_operation()
        except StopIteration:
            pass

        self.assertEquals(4, self.a2.get_count())
        self.assertAlmostEqual(self.a2.get_average(), 0.02, places=2 )
        self.assertNotEquals(0, self.a2.get_standard_deviation())
        self.assertAlmostEqual(self.a2.get_average('one'), 0.0075, places=3 )


if __name__ == '__main__':
    unittest.main()