
import os
from unittest import TestCase,main
import ooi.testing
import __main__

class TestOOIImports(ooi.testing.ImportTest):
    def __init__(self,*a,**b):
        # for utilities project only, want to search in BASE/src
        # but this test is in BASE/test/ooi
        # so have to go up two levels, then down to src
        target_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))),'src')
        super(TestOOIImports,self).__init__(target_dir, 'ooi', *a, **b)

if __name__ == '__main__':
    main()