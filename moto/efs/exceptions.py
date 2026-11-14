from moto.core.exceptions import ServiceException


class EFSError(ServiceException):
    pass


class AccessPointNotFound(EFSError):
    code = "AccessPointNotFound"

    def __init__(self, access_point_id: str):
        super().__init__(f"Access Point {access_point_id} does not exist.")


class FileSystemAlreadyExists(EFSError):
    code = "FileSystemAlreadyExists"

    def __init__(self, creation_token: str):
        super().__init__(f"File system with {creation_token} already exists.")


class FileSystemNotFound(EFSError):
    code = "FileSystemNotFound"

    def __init__(self, file_system_id: str):
        super().__init__(f"File system '{file_system_id}' does not exist.")


class FileSystemInUse(EFSError):
    code = "FileSystemInUse"


class MountTargetConflict(EFSError):
    code = "MountTargetConflict"


class MountTargetNotFound(EFSError):
    code = "MountTargetNotFound"

    def __init__(self, mount_target_id: str):
        super().__init__(f"Mount target '{mount_target_id}' does not exist.")


class BadRequest(EFSError):
    code = "BadRequest"


class PolicyNotFound(EFSError):
    code = "PolicyNotFound"


class SubnetNotFound(EFSError):
    code = "SubnetNotFound"

    def __init__(self, subnet_id: str):
        super().__init__(f"The subnet ID '{subnet_id}' does not exist")


class SecurityGroupNotFound(EFSError):
    code = "SecurityGroupNotFound"

    def __init__(self, security_group_id: str):
        super().__init__(f"The SecurityGroup ID '{security_group_id}' does not exist")


class SecurityGroupLimitExceeded(EFSError):
    code = "SecurityGroupLimitExceeded"
