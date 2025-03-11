from moto.core.exceptions import MotoServiceException


class LifecyclePolicyNotFoundException(MotoServiceException):
    code = "LifecyclePolicyNotFoundException"

    def __init__(self, repository_name: str, registry_id: str):
        message = (
            f"Lifecycle policy does not exist "
            f"for the repository with name '{repository_name}' "
            f"in the registry with id '{registry_id}'"
        )
        super().__init__(message)


class LimitExceededException(MotoServiceException):
    code = "LimitExceededException"
    message = "The scan quota per image has been exceeded. Wait and try again."


class RegistryPolicyNotFoundException(MotoServiceException):
    code = "RegistryPolicyNotFoundException"
    message = "Registry policy does not exist in the registry with id '{registry_id}'"

    def __init__(self, registry_id: str):
        super().__init__(registry_id=registry_id)


class RepositoryAlreadyExistsException(MotoServiceException):
    code = "RepositoryAlreadyExistsException"

    def __init__(self, repository_name: str, registry_id: str):
        message = (
            f"The repository with name '{repository_name}' already exists "
            f"in the registry with id '{registry_id}'"
        )
        super().__init__(message)


class RepositoryNotEmptyException(MotoServiceException):
    code = "RepositoryNotEmptyException"

    def __init__(self, repository_name: str, registry_id: str):
        message = (
            f"The repository with name '{repository_name}' "
            f"in registry with id '{registry_id}' "
            f"cannot be deleted because it still contains images"
        )
        super().__init__(message)


class RepositoryNotFoundException(MotoServiceException):
    code = "RepositoryNotFoundException"

    def __init__(self, repository_name: str, registry_id: str):
        message = (
            f"The repository with name '{repository_name}' does not exist "
            f"in the registry with id '{registry_id}'"
        )
        super().__init__(message)


class RepositoryPolicyNotFoundException(MotoServiceException):
    code = "RepositoryPolicyNotFoundException"

    def __init__(self, repository_name: str, registry_id: str):
        message = (
            f"Repository policy does not exist "
            f"for the repository with name '{repository_name}' "
            f"in the registry with id '{registry_id}'"
        )
        super().__init__(message)


class ImageNotFoundException(MotoServiceException):
    code = "ImageNotFoundException"

    def __init__(self, image_id: str, repository_name: str, registry_id: str):
        message = (
            f"The image with imageId {image_id} does not exist "
            f"within the repository with name '{repository_name}' "
            f"in the registry with id '{registry_id}'"
        )
        super().__init__(message)


class ImageAlreadyExistsException(MotoServiceException):
    code = "ImageAlreadyExistsException"

    def __init__(
        self,
        repository_name: str,
        registry_id: str,
        digest: str,
        image_tag: str,
    ):
        message = (
            f"Image with digest '{digest}' and tag '{image_tag}' already exists "
            f"in the repository with name '{repository_name}' "
            f"in registry with id '{registry_id}'"
        )
        super().__init__(message)


class InvalidParameterException(MotoServiceException):
    code = "InvalidParameterException"


class ScanNotFoundException(MotoServiceException):
    code = "ScanNotFoundException"

    def __init__(self, image_id: str, repository_name: str, registry_id: str):
        message = (
            f"Image scan does not exist for the image with '{image_id}' "
            f"in the repository with name '{repository_name}' "
            f"in the registry with id '{registry_id}'"
        )
        super().__init__(message)


class ValidationException(MotoServiceException):
    code = "ValidationException"
