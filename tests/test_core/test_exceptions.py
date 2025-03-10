from moto.core.exceptions import MotoServiceException


class TestMotoServiceException:
    class TestException(MotoServiceException):
        code = "ExceptionCode"
        message = "default message"

    def test_exception_string(self) -> None:
        exc = TestMotoServiceException.TestException()
        assert str(exc) == "ExceptionCode: default message"

    def test_override_exception_message(self) -> None:
        exc = TestMotoServiceException.TestException("Override message")
        assert str(exc) == "ExceptionCode: Override message"
