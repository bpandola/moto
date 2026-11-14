from moto.core.exceptions import ServiceException


class WAFv2ClientError(ServiceException):
    pass


class WAFV2DuplicateItemException(WAFv2ClientError):
    code = "WafV2DuplicateItem"
    message = "AWS WAF could not perform the operation because some resource in your request is a duplicate of an existing one."


class WAFV2InsufficientInformationException(WAFv2ClientError):
    code = "AcessDeniedException"

    def __init__(
        self,
        name: str | None,
        scope: str | None,
        id: str | None,
        arn: str | None,
    ) -> None:
        super().__init__(
            "Critical information is missing in your request: "
            f"GetRuleGroupRequest(name={name if name else 'null'}, "
            f"scope={scope if scope else 'null'}, "
            f"id={id if id else 'null'}, "
            f"aRN={arn if arn is not None else 'null'})."
        )


class WAFNonexistentItemException(WAFv2ClientError):
    code = "WAFNonexistentItemException"
    message = (
        "AWS WAF couldn’t perform the operation because your resource doesn’t exist."
    )


class WAFOptimisticLockException(WAFv2ClientError):
    code = "WAFOptimisticLockException"
    message = "AWS WAF couldn’t save your changes because someone changed the resource after you started to edit it. Reapply your changes."
