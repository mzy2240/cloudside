import sys
import matplotlib
matplotlib.use('agg')

from matplotlib import pyplot
pyplot.style.use('classic')

import cloudside


status = cloudside.test(*sys.argv[1:])
sys.exit(status)
