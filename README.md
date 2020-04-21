# Project Directory Structure
      CaseStudy 
            Project 
                __init__.py
                
            Test
                __init__.py
                test_error.py
            run.py



# Project Explanation
Main directory name is __Case Study__ under this three files are present Project, run.py and Test.

Project -> It is a package and contains the code for the API.

run.py -> Python file to run the application.

Test -> It is a package and contains the test case for testing the API. __"test_error.py"__ contains the all the code for testing the API.

## Step 1
Run the __"run.py"__. It will run the application created in "__init__.py" in project package. In "__init__.py", Firsty we connect to the database with the help of pymongo driver, it created an instance of __MongoClient__ and then read the data from the database.

A root-end point for API request is also created in this. Once the request is received from the API, it executed its corresponding function and return the response according to the corresponding request.

## Step 2
It contains all the test cases to test the API. Run the command __"pytest -v"__to see the result.
