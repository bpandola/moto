from moto.core.exceptions import MotoServiceException


class LifecyclePolicyNotFoundException(MotoServiceException):
    code = "LifecyclePolicyNotFoundException"
    message = (
        "Lifecycle policy does not exist "
        "for the repository with name '{repository_name}' "
        "in the registry with id '{registry_id}'"
    )

    def __init__(self, repository_name: str, registry_id: str):
        super().__init__(repository_name=repository_name, registry_id=registry_id)


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
    message = (
        "The repository with name '{repository_name}' already exists "
        "in the registry with id '{registry_id}'"
    )

    def __init__(self, repository_name: str, registry_id: str):
        super().__init__(repository_name=repository_name, registry_id=registry_id)


class RepositoryNotEmptyException(MotoServiceException):
    code = "RepositoryNotEmptyException"
    message = (
        "The repository with name '{repository_name}' "
        "in registry with id '{registry_id}' "
        "cannot be deleted because it still contains images"
    )

    def __init__(self, repository_name: str, registry_id: str):
        super().__init__(repository_name=repository_name, registry_id=registry_id)


class RepositoryNotFoundException(MotoServiceException):
    code = "RepositoryNotFoundException"
    message = (
        "The repository with name '{repository_name}' does not exist "
        "in the registry with id '{registry_id}'"
    )

    def __init__(self, repository_name: str, registry_id: str):
        super().__init__(repository_name=repository_name, registry_id=registry_id)


class RepositoryPolicyNotFoundException(MotoServiceException):
    code = "RepositoryPolicyNotFoundException"
    message = (
        "Repository policy does not exist "
        "for the repository with name '{repository_name}' "
        "in the registry with id '{registry_id}'"
    )

    def __init__(self, repository_name: str, registry_id: str):
        super().__init__(repository_name=repository_name, registry_id=registry_id)


class ImageNotFoundException(MotoServiceException):
    code = "ImageNotFoundException"
    message = (
        "The image with imageId {image_id} does not exist "
        "within the repository with name '{repository_name}' "
        "in the registry with id '{registry_id}'"
    )

    def __init__(self, image_id: str, repository_name: str, registry_id: str):
        super().__init__(
            image_id=image_id, repository_name=repository_name, registry_id=registry_id
        )


class ImageAlreadyExistsException(MotoServiceException):
    code = "ImageAlreadyExistsException"
    message = (
        "Image with digest '{digest}' and tag '{image_tag}' already exists "
        "in the repository with name '{repository_name}' "
        "in registry with id '{registry_id}'"
    )

    def __init__(
        self,
        repository_name: str,
        registry_id: str,
        digest: str,
        image_tag: str,
    ):
        super().__init__(
            repository_name=repository_name,
            registry_id=registry_id,
            digest=digest,
            image_tag=image_tag,
        )


class InvalidParameterException(MotoServiceException):
    code = "InvalidParameterException"


class ScanNotFoundException(MotoServiceException):
    code = "ScanNotFoundException"
    message = (
        "Image scan does not exist for the image with '{image_id}' "
        "in the repository with name '{repository_name}' "
        "in the registry with id '{registry_id}'"
    )

    def __init__(self, image_id: str, repository_name: str, registry_id: str):
        super().__init__(
            image_id=image_id, repository_name=repository_name, registry_id=registry_id
        )


class ValidationException(MotoServiceException):
    code = "ValidationException"
