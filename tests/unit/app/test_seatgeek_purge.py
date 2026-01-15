import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch
from src.app.main import app

client = TestClient(app)

@pytest.fixture
def admin_user_token():
    # This should be replaced with a valid admin JWT for your app
    return "Bearer test-admin-token"

@patch("src.app.db.seatgeek_account_db.get_seatgeek_account_data")
@patch("src.app.db.seatgeek_account_db.send_seatgeek_purge_message")
def test_purge_seatgeek_listings_success(mock_send, mock_get, admin_user_token):
    mock_get.return_value = {"token": "test-token"}
    mock_send.return_value = "mock-message-id"
    payload = {"account_id": "test-account", "purge_flag": True}
    response = client.post(
        "/seatgeek/purge/listings",
        json=payload,
        headers={"Authorization": admin_user_token}
    )
    assert response.status_code == 200
    assert response.json()["status"] == "success"
    assert response.json()["message_id"] == "mock-message-id"

@patch("src.app.db.seatgeek_account_db.get_seatgeek_account_data")
@patch("src.app.db.seatgeek_account_db.send_seatgeek_purge_message")
def test_purge_seatgeek_listings_sqs_fail(mock_send, mock_get, admin_user_token):
    mock_get.return_value = {"token": "test-token"}
    mock_send.return_value = None
    payload = {"account_id": "test-account", "purge_flag": True}
    response = client.post(
        "/seatgeek/purge/listings",
        json=payload,
        headers={"Authorization": admin_user_token}
    )
    assert response.status_code == 500
    assert response.json()["detail"] == "Failed to send purge request to SeatGeek SQS queue."
