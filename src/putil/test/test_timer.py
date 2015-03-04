import time
from math import fabs

import putil.timer
from putil.testing import UtilTest


class TestTimer(UtilTest):
    def setUp(self):
        self.op1_times = iter([ .01, .02 ])
        self.a1 = putil.timer.Accumulator()

        self.op2_step1_times = iter([ .005, .015, .005, .005])
        self.op2_step2_times = iter([ .01, .02, .01, .01])
        self.a2 = putil.timer.Accumulator()

    def test_found_caller(self):
        import importable.create_timer
        t = importable.create_timer.t
        self.assertEquals('timing.putil.test.importable.create_timer', t.logger.name)

    def test_time_event(self):
        t = putil.timer.Timer()

        time.sleep(0.01)
        t.complete_step('pause')

        time.sleep(0.02)
        t.complete_step()

        self.assertEquals(3, len(t.times))

    def one_step_operation(self):
        t = putil.timer.Timer()
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
        self.assertAlmostEqual(self.a1.get_average(), 0.015, places=2)
        self.assertTrue( fabs(self.a1.get_average()-0.015) < .002 )
        self.assertAlmostEqual(self.a1.get_standard_deviation(), 0.005, places=2)

    def two_step_operation(self):
        t = putil.timer.Timer()
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

        self.assertEquals(8, self.a2.get_count())
        self.assertEquals(4, self.a2.get_count("one"))
        self.assertEquals(4, self.a2.get_count("two"))

        self.assertAlmostEqual(self.a2.get_average(), 0.01, places=2)
        self.assertAlmostEqual(self.a2.get_average("one"), 0.008, places=2)
        self.assertAlmostEqual(self.a2.get_average("two"), 0.013, places=2)

        self.assertNotEquals(0, self.a2.get_standard_deviation())
