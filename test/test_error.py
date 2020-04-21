import unittest
from project import app

class ErrorTests(unittest.TestCase):
	@classmethod
	def setUpClass(cls):
		pass

	@classmethod
	def tearDownClass(cls):
		pass

	def setUp(self):
		self.app = app.test_client()
		self.app.testing = True

	def test_pagenotfound_statuscode1(self):
		result = self.app.get("/invalid/url")

		self.assertEqual(result.status_code, 404)
		
	def test_pagenotfound_statuscode1(self):
		result = self.app.get("/api/tasks/1?country=india")

		self.assertNotEqual(result.status_code, 404)

	def test_pagenotfound_statuscode2(self):
		result = self.app.get("/api/tasks/2?date=2020-04-15")

		self.assertNotEqual(result.status_code, 404)

	def test_pagenotfound_statuscode3(self):
		result = self.app.get("/api/tasks/3?top=100")

		self.assertNotEqual(result.status_code, 404)

	def test_pagenotfound_statuscode3(self):
		result = self.app.get("/api/tasks/4")

		self.assertEqual(result.status_code, 200)

	def test_pagenotfound_statuscode3(self):
		result = self.app.get("/api/tasks/3?country=all")

		self.assertEqual(result.status_code, 200)
	#This method is responsible for clearing our infrastructure setup.
	def tearDown(self):
		pass

