from moto.core.exceptions import ServiceException


class CodeCommitException(ServiceException):
    pass


class RepositoryNameExistsException(CodeCommitException):
    code = "RepositoryNameExistsException"

    def __init__(self, repository_name: str):
        super().__init__(f"Repository named {repository_name} already exists")


class RepositoryDoesNotExistException(CodeCommitException):
    code = "RepositoryDoesNotExistException"

    def __init__(self, repository_name: str):
        super().__init__(f"{repository_name} does not exist")


class InvalidRepositoryNameException(CodeCommitException):
    code = "InvalidRepositoryNameException"
    message = (
        "The repository name is not valid. Repository names can be any valid "
        "combination of letters, numbers, "
        "periods, underscores, and dashes between 1 and 100 characters in "
        "length. Names are case sensitive. "
        "For more information, see Limits in the AWS CodeCommit User Guide. "
    )
