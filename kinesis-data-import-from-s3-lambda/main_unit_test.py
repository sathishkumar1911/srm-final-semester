from main import lambda_handler

def test_execute():
    lambda_handler({
        "Records": [
            {
                "s3": {
                    "bucket": {
                        "name": "test-bucket"
                    },
                    "object": {
                        "key": "testObject.csv"
                    },
                    "isTest": True
                }
            }
        ]
    }, "")

test_execute()