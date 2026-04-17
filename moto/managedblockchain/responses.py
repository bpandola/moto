import json

from moto.core.responses import ActionResult, BaseResponse, EmptyResult

from .models import ManagedBlockchainBackend, managedblockchain_backends


class ManagedBlockchainResponse(BaseResponse):
    def __init__(self) -> None:
        super().__init__(service_name="managedblockchain")
        self.automated_parameter_parsing = True

    @property
    def backend(self) -> ManagedBlockchainBackend:
        return managedblockchain_backends[self.current_account][self.region]

    def list_networks(self) -> ActionResult:
        networks = self.backend.list_networks()
        return ActionResult(result={"Networks": networks})

    def create_network(self) -> str:
        name = self._get_param("Name")
        framework = self._get_param("Framework")
        frameworkversion = self._get_param("FrameworkVersion")
        frameworkconfiguration = self._get_param("FrameworkConfiguration")
        voting_policy = self._get_param("VotingPolicy")
        member_configuration = self._get_param("MemberConfiguration")

        # Optional
        description = self._get_param("Description", None)

        response = self.backend.create_network(
            name,
            framework,
            frameworkversion,
            frameworkconfiguration,
            voting_policy,
            member_configuration,
            description,
        )
        return json.dumps(response)

    def get_network(self) -> ActionResult:
        network_id = self._get_param("NetworkId")
        mbcnetwork = self.backend.get_network(network_id)
        return ActionResult(result={"Network": mbcnetwork})

    def list_proposals(self) -> ActionResult:
        network_id = self._get_param("NetworkId")
        proposals = self.backend.list_proposals(network_id)
        return ActionResult(result={"Proposals": proposals})

    def create_proposal(self) -> str:
        network_id = self._get_param("NetworkId")
        memberid = self._get_param("MemberId")
        actions = self._get_param("Actions")

        # Optional
        description = self._get_param("Description", None)

        response = self.backend.create_proposal(
            network_id, memberid, actions, description
        )
        return json.dumps(response)

    def get_proposal(self) -> ActionResult:
        network_id = self._get_param("NetworkId")
        proposal_id = self._get_param("ProposalId")
        proposal = self.backend.get_proposal(network_id, proposal_id)
        return ActionResult(result={"Proposal": proposal})

    def list_proposal_votes(self) -> str:
        network_id = self._get_param("NetworkId")
        proposal_id = self._get_param("ProposalId")
        proposalvotes = self.backend.list_proposal_votes(network_id, proposal_id)
        return json.dumps({"ProposalVotes": proposalvotes})

    def vote_on_proposal(self) -> ActionResult:
        network_id = self._get_param("NetworkId")
        proposal_id = self._get_param("ProposalId")
        votermemberid = self._get_param("VoterMemberId")
        vote = self._get_param("Vote")

        self.backend.vote_on_proposal(network_id, proposal_id, votermemberid, vote)
        return EmptyResult()

    def list_invitations(self) -> ActionResult:
        invitations = self.backend.list_invitations()
        return ActionResult(result={"Invitations": invitations})

    def reject_invitation(self) -> str:
        invitation_id = self._get_param("InvitationId")
        self.backend.reject_invitation(invitation_id)
        return ""

    def list_members(self) -> ActionResult:
        network_id = self._get_param("NetworkId")
        members = self.backend.list_members(network_id)
        return ActionResult(result={"Members": members})

    def create_member(self) -> str:
        network_id = self._get_param("NetworkId")
        invitationid = self._get_param("InvitationId")
        member_configuration = self._get_param("MemberConfiguration")

        response = self.backend.create_member(
            invitationid, network_id, member_configuration
        )
        return json.dumps(response)

    def get_member(self) -> ActionResult:
        network_id = self._get_param("NetworkId")
        member_id = self._get_param("MemberId")
        member = self.backend.get_member(network_id, member_id)
        return ActionResult(result={"Member": member})

    def update_member(self) -> str:
        network_id = self._get_param("NetworkId")
        member_id = self._get_param("MemberId")
        logpublishingconfiguration = self._get_param("LogPublishingConfiguration")
        self.backend.update_member(network_id, member_id, logpublishingconfiguration)
        return ""

    def delete_member(self) -> str:
        network_id = self._get_param("NetworkId")
        member_id = self._get_param("MemberId")
        self.backend.delete_member(network_id, member_id)
        return ""

    def list_nodes(self) -> ActionResult:
        network_id = self._get_param("NetworkId")
        member_id = self._get_param("MemberId")
        status = self._get_param("Status")
        nodes = self.backend.list_nodes(network_id, member_id, status)
        return ActionResult(result={"Nodes": nodes})

    def create_node(self) -> str:
        network_id = self._get_param("NetworkId")
        member_id = self._get_param("MemberId")
        instancetype = self._get_param("NodeConfiguration")["InstanceType"]
        availabilityzone = self._get_param("NodeConfiguration")["AvailabilityZone"]
        logpublishingconfiguration = self._get_param("NodeConfiguration")[
            "LogPublishingConfiguration"
        ]

        response = self.backend.create_node(
            network_id,
            member_id,
            availabilityzone,
            instancetype,
            logpublishingconfiguration,
        )
        return json.dumps(response)

    def get_node(self) -> ActionResult:
        network_id = self._get_param("NetworkId")
        member_id = self._get_param("MemberId")
        node_id = self._get_param("NodeId")
        node = self.backend.get_node(network_id, member_id, node_id)
        return ActionResult(result={"Node": node})

    def update_node(self) -> str:
        network_id = self._get_param("NetworkId")
        member_id = self._get_param("MemberId")
        node_id = self._get_param("NodeId")
        self.backend.update_node(
            network_id, member_id, node_id, logpublishingconfiguration=self.body
        )
        return ""

    def delete_node(self) -> str:
        network_id = self._get_param("NetworkId")
        member_id = self._get_param("MemberId")
        node_id = self._get_param("NodeId")
        self.backend.delete_node(network_id, member_id, node_id)
        return ""
