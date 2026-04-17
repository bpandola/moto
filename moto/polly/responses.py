import json
import re
from typing import Any, Union

from moto.core.responses import BaseResponse

from .models import PollyBackend, polly_backends
from .resources import LANGUAGE_CODES, VOICE_IDS

LEXICON_NAME_REGEX = re.compile(r"^[0-9A-Za-z]{1,20}$")


class PollyResponse(BaseResponse):
    def __init__(self) -> None:
        super().__init__(service_name="polly")
        self.automated_parameter_parsing = True

    @property
    def polly_backend(self) -> PollyBackend:
        return polly_backends[self.current_account][self.region]

    def _error(self, code: str, message: str) -> tuple[str, dict[str, int]]:
        return json.dumps({"__type": code, "message": message}), {"status": 400}

    # DescribeVoices
    def describe_voices(self) -> Union[str, tuple[str, dict[str, int]]]:
        language_code = self._get_param("LanguageCode")

        if language_code is not None and language_code not in LANGUAGE_CODES:
            all_codes = ", ".join(LANGUAGE_CODES)  # type: ignore
            msg = (
                f"1 validation error detected: Value '{language_code}' at 'languageCode' failed to satisfy constraint: "
                f"Member must satisfy enum value set: [{all_codes}]"
            )
            return msg, {"status": 400}

        voices = self.polly_backend.describe_voices(language_code)

        return json.dumps({"Voices": voices})

    # PutLexicon
    def put_lexicon(self) -> Union[str, tuple[str, dict[str, int]]]:
        lexicon_name = self._get_param("Name")

        if LEXICON_NAME_REGEX.match(lexicon_name) is None:
            return self._error(
                "InvalidParameterValue", "Lexicon name must match [0-9A-Za-z]{1,20}"
            )

        if "Content" not in self._get_params():
            return self._error("MissingParameter", "Content is missing from the body")

        self.polly_backend.put_lexicon(lexicon_name, self._get_params()["Content"])

        return ""

    # ListLexicons
    def list_lexicons(self) -> str:
        result = {"Lexicons": self.polly_backend.list_lexicons()}

        return json.dumps(result)

    # GetLexicon
    def get_lexicon(self) -> Union[str, tuple[str, dict[str, int]]]:
        lexicon_name = self._get_param("Name")

        try:
            lexicon = self.polly_backend.get_lexicon(lexicon_name)
        except KeyError:
            return self._error("LexiconNotFoundException", "Lexicon not found")

        result = {
            "Lexicon": {"Name": lexicon_name, "Content": lexicon.content},
            "LexiconAttributes": lexicon.to_dict()["Attributes"],
        }

        return json.dumps(result)

    # DeleteLexicon
    def delete_lexicon(self) -> Union[str, tuple[str, dict[str, int]]]:
        lexicon_name = self._get_param("Name")

        try:
            self.polly_backend.delete_lexicon(lexicon_name)
        except KeyError:
            return self._error("LexiconNotFoundException", "Lexicon not found")

        return ""

    # SynthesizeSpeech
    def synthesize_speech(self) -> tuple[str, dict[str, Any]]:
        params = self._get_params()
        # Sanity check params
        args = {
            "lexicon_names": None,
            "sample_rate": 22050,
            "speech_marks": None,
            "text": None,
            "text_type": "text",
        }

        if "LexiconNames" in params:
            for lex in params["LexiconNames"]:
                try:
                    self.polly_backend.get_lexicon(lex)
                except KeyError:
                    return self._error("LexiconNotFoundException", "Lexicon not found")

            args["lexicon_names"] = params["LexiconNames"]

        if "OutputFormat" not in params:
            return self._error("MissingParameter", "Missing parameter OutputFormat")
        if params["OutputFormat"] not in ("json", "mp3", "ogg_vorbis", "pcm"):
            return self._error(
                "InvalidParameterValue", "Not one of json, mp3, ogg_vorbis, pcm"
            )
        args["output_format"] = params["OutputFormat"]

        if "SampleRate" in params:
            sample_rate = int(params["SampleRate"])
            if sample_rate not in (8000, 16000, 22050):
                return self._error(
                    "InvalidSampleRateException",
                    "The specified sample rate is not valid.",
                )
            args["sample_rate"] = sample_rate

        if "SpeechMarkTypes" in params:
            for value in params["SpeechMarkTypes"]:
                if value not in ("sentance", "ssml", "viseme", "word"):
                    return self._error(
                        "InvalidParameterValue",
                        "Not one of sentance, ssml, viseme, word",
                    )
            args["speech_marks"] = params["SpeechMarkTypes"]

        if "Text" not in params:
            return self._error("MissingParameter", "Missing parameter Text")
        args["text"] = params["Text"]

        if "TextType" in params:
            if params["TextType"] not in ("ssml", "text"):
                return self._error("InvalidParameterValue", "Not one of ssml, text")
            args["text_type"] = params["TextType"]

        if "VoiceId" not in params:
            return self._error("MissingParameter", "Missing parameter VoiceId")
        if params["VoiceId"] not in VOICE_IDS:
            all_voices = ", ".join(VOICE_IDS)  # type: ignore
            return self._error("InvalidParameterValue", f"Not one of {all_voices}")
        args["voice_id"] = params["VoiceId"]

        # More validation
        if len(args["text"]) > 3000:  # type: ignore
            return self._error("TextLengthExceededException", "Text too long")

        if args["speech_marks"] is not None and args["output_format"] != "json":
            return self._error(
                "MarksNotSupportedForFormatException", "OutputFormat must be json"
            )
        if args["speech_marks"] is not None and args["text_type"] == "text":
            return self._error(
                "SsmlMarksNotSupportedForTextTypeException", "TextType must be ssml"
            )

        content_type = "audio/json"
        if args["output_format"] == "mp3":
            content_type = "audio/mpeg"
        elif args["output_format"] == "ogg_vorbis":
            content_type = "audio/ogg"
        elif args["output_format"] == "pcm":
            content_type = "audio/pcm"

        headers = {"Content-Type": content_type}

        return "\x00\x00\x00\x00\x00\x00\x00\x00", headers
