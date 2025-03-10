from moto.core.exceptions import MotoServiceException


class TestMotoServiceException:
    class TestException(MotoServiceException):
        code = "ExceptionCode"
        message = "default message"

    def test_exception_string(self) -> None:
        exc = TestMotoServiceException.TestException()
        assert str(exc) == "ExceptionCode: default message"

    def test_formatted_exception_message(self) -> None:
        class FormattedException(MotoServiceException):
            message = "The {resource_type} resource {resource_id} was not found!"

        exc = FormattedException(resource_type="DBCluster", resource_id="cluster-id")
        assert exc.message == "The DBCluster resource cluster-id was not found!"

    def test_overridden_exception_message(self) -> None:
        exc = TestMotoServiceException.TestException("Override message")
        assert str(exc) == "ExceptionCode: Override message"
