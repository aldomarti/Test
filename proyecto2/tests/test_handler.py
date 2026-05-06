"""Test handler person-connect"""
import json
from src.handler import lambda_handler


def test_lambda_handler_success():
    event = {}
    context = {}
    response = lambda_handler(event, context)
    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert "message" in body
