import unittest

suite = unittest.TestLoader().discover('.', pattern='*_test.py')
unittest.TextTestRunner().run(suite)

