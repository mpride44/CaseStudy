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

	def tearDown(self):
		pass

	def test_pagenotfound_statuscode1(self):
		result = self.app.get("/invalid/url")

		self.assertEqual(result.status_code, 404)

	def test_pagenotfound_statuscode3(self):
		result = self.app.get("/api/tasks/2")

		self.assertNotEqual(result.status_code, 404)

	def test_pagenotfound_statuscode4(self):
		result = self.app.get("/api/tasks/1")

		self.assertNotEqual(result.status_code, 404)

	def test_pagenotfound_statuscode5(self):
		result = self.app.get("/api/tasks/3")

		self.assertNotEqual(result.status_code, 404)
