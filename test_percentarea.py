from modules.hms.percent_area import CatchmentGrid
import unittest
import warnings
import json
import time

print("test_percentarea.py")


class PercentAreaTest(unittest.TestCase):
	"""
	Unit tests for spatially weighted average data
	"""
	def test_huc8(self):
		"""
		Test a sample Huc 8
		"""
		warnings.simplefilter("ignore", ResourceWarning)
		try:
			result = CatchmentGrid.getIntersectCellsInHuc8('03050204', 'nldas')
			self.assertIsNotNone(result)
			jsonData = json.loads(result)
			self.assertEqual(1860, jsonData['metadata']['number of points'])
		except Exception as e:
			print("Huc 8 test failed... exception: {}".format(e))
			return False

	def test_huc12(self):
		"""
		Test a sample Huc 12
		"""
		warnings.simplefilter("ignore", ResourceWarning)
		try:
			result = CatchmentGrid.getIntersectCellsInHuc12('030502040102', 'nldas')
			self.assertIsNotNone(result)
			jsonData = json.loads(result)
			self.assertEqual(100, jsonData['metadata']['number of points'])
		except Exception as e:
			print("Huc 12 test failed... exception: {}".format(e))
			return False

	def test_comList(self):
		"""
		Test a sample List of COM ID's
		"""
		warnings.simplefilter("ignore", ResourceWarning)
		try:
			result = CatchmentGrid.getIntersectCellsInComlist('9311811,9311813,9311815,9311817,9311819', 'nldas')
			self.assertIsNotNone(result)
			jsonData = json.loads(result)
			self.assertEqual(7, jsonData['metadata']['number of points'])
		except Exception as e:
			print("List of COM ID's test failed... exception: {}".format(e))
			return False

	def test_comID(self):
		"""
		Test a sample COM ID
		"""
		warnings.simplefilter("ignore", ResourceWarning)
		try:
			result = CatchmentGrid.getIntersectCellsInComlist('9311811', 'nldas')
			self.assertIsNotNone(result)
			jsonData = json.loads(result)
			self.assertEqual(1, jsonData['metadata']['number of points'])
		except Exception as e:
			print("COM ID test failed... exception: {}".format(e))
			return False

	"""
	Test each of NLDAS, GLDAS, DAYMET, and PRISM grids with Huc 8 01010010
	"""
	def test_nldas(self):
		warnings.simplefilter("ignore", ResourceWarning)
		try:
			nldasResult = CatchmentGrid.getIntersectCellsInHuc8('01010010', 'nldas')
			self.assertIsNotNone(nldasResult, 'NLDAS error')
			nldasJsonData = json.loads(nldasResult)
			self.assertEqual(224, nldasJsonData['metadata']['number of points'])
		except Exception as e:
			print("NLDAS grid test failed... exception: {}".format(e))
			return False

	def test_gldas(self):
		warnings.simplefilter("ignore", ResourceWarning)
		try:
			gldasResult = CatchmentGrid.getIntersectCellsInHuc8('01010010', 'gldas')
			self.assertIsNotNone(gldasResult, 'GLDAS error')
			gldasJsonData = json.loads(gldasResult)
			self.assertEqual(181, gldasJsonData['metadata']['number of points'])
		except Exception as e:
			print("GLDAS grid test failed... exception: {}".format(e))
			return False

	def test_daymet(self):
		warnings.simplefilter("ignore", ResourceWarning)
		try:
			daymetResult = CatchmentGrid.getIntersectCellsInHuc8('01010010', 'daymet')
			self.assertIsNotNone(daymetResult, 'Daymet error')
			daymetJsonData = json.loads(daymetResult)
			self.assertEqual(162, daymetJsonData['metadata']['number of points'])
		except Exception as e:
			print("Daymet grid test failed... exception: {}".format(e))
			return False

	def test_prism(self):
		warnings.simplefilter("ignore", ResourceWarning)
		try:
			prismResult = CatchmentGrid.getIntersectCellsInHuc8('01010010', 'prism')
			self.assertIsNotNone(prismResult, 'PRISM error')
			prismJsonData = json.loads(prismResult)
			self.assertEqual(384, prismJsonData['metadata']['number of points'])
		except Exception as e:
			print("PRISM grid test failed... exception: {}".format(e))
			return False
	"""
	def sample_test(self):
		warnings.simplefilter("ignore", ResourceWarning)
		try:
			prismResult = CatchmentGrid.getIntersectCellsInComlist('49662', 'prism')
			self.assertIsNotNone(prismResult, 'PRISM error')
			prismJsonData = json.loads(prismResult)
			self.assertEqual(4, prismJsonData['metadata']['number of points'])
		except Exception as e:
			print("PRISM grid test failed... exception: {}".format(e))
			return False
	"""

if __name__ == '__main__':
    unittest.main()