import unittest
from world.flat import Earth

class Test_EarthTestCase(unittest.TestCase):
    def test_hello(self):
        earth = Earth()
        self.assertEqual('world', earth.getWord())

if __name__ == '__main__':
    unittest.main()
