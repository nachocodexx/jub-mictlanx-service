import pytest
import httpx



def test_http_exception():
    with pytest.raises(httpx.HTTPError):
        raise httpx.HTTPError("An error occurred")

def test_timeout_exception():
    with pytest.raises(httpx.TimeoutException):
        raise httpx.TimeoutException("Request timed out")
def test_request_exception():    
    with pytest.raises(httpx.RequestError):
        raise httpx.RequestError("An error occurred while making the request")

def test_connection_error():   
    with pytest.raises(httpx.ConnectError):
        raise httpx.ConnectError("Failed to connect to the server")
def test_read_error():    
    with pytest.raises(httpx.ReadError):
        raise httpx.ReadError("An error occurred while reading the response")
def test_write_error():    
    with pytest.raises(httpx.WriteError):
        raise httpx.WriteError("An error occurred while writing the request")
def test_decoding_error():    
    with pytest.raises(httpx.DecodingError):
        raise httpx.DecodingError("An error occurred while decoding the response")
def test_connect_timeout():    
    with pytest.raises(httpx.ConnectTimeout):
        raise httpx.ConnectTimeout("Connection timed out")
def test_connect_timeout_2():    
    with pytest.raises(httpx.ConnectTimeout):
        raise httpx._exceptions.ConnectTimeout("Connection timed out")