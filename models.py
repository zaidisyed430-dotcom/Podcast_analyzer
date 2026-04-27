import re
import json
import os

_CONFIG_PATH = "models_config.json"

# Default pre-trained models used when no fine-tuned weights are present
_DEFAULT_SENTIMENT = "cardiffnlp/twitter-roberta-base-sentiment-latest"
_DEFAULT_SPAM      = "distilbert-base-uncased-finetuned-sst-2-english"

_SENTIMENT_LABEL_MAP = {
    "positive": "positive", "negative": "negative", "neutral": "neutral",
    "LABEL_0": "negative",  "LABEL_1": "neutral",   "LABEL_2": "positive",
    "POSITIVE": "positive", "NEGATIVE": "negative",
}


def _load_config() -> dict:
    """
    Read models_config.json.  If the config claims fine_tuned=True but the
    model directories don't actually exist on disk, fall back to base models
    so the app doesn't crash when weights haven't been downloaded yet.
    """
    if not os.path.exists(_CONFIG_PATH):
        return {"fine_tuned": False}

    with open(_CONFIG_PATH) as f:
        config = json.load(f)

    if config.get("fine_tuned"):
        s_path = config.get("sentiment_model", "")
        p_path = config.get("spam_model", "")
        s_ok   = os.path.isdir(s_path) and bool(os.listdir(s_path))
        p_ok   = os.path.isdir(p_path) and bool(os.listdir(p_path))

        if not s_ok and not p_ok:
            print(
                "[models] models_config.json says fine_tuned=True "
                "but model directories are missing. "
                "Extract podcastlens_models.zip into the project root, "
                "then restart the server."
            )
            config["fine_tuned"] = False

    return config


class CommentAnalyzer:
    def __init__(self):
        self.sentiment_pipeline = None
        self.spam_pipeline      = None
        self.models_loaded      = False
        self._using_fine_tuned  = False

    def load_models(self):
        from transformers import pipeline

        config = _load_config()

        if config.get("fine_tuned"):
            sentiment_model = config["sentiment_model"]
            spam_model      = config["spam_model"]
            self._using_fine_tuned = True
            print(f"[models] Loading fine-tuned sentiment : {sentiment_model}")
            print(f"[models] Loading fine-tuned spam      : {spam_model}")
        else:
            sentiment_model = _DEFAULT_SENTIMENT
            spam_model      = _DEFAULT_SPAM
            self._using_fine_tuned = False
            print(f"[models] Loading base sentiment model : {sentiment_model}")
            print(f"[models] Loading base spam model      : {spam_model}")

        self.sentiment_pipeline = pipeline(
            "text-classification",
            model=sentiment_model,
            top_k=None,
            truncation=True,
            max_length=512,
        )

        self.spam_pipeline = pipeline(
            "text-classification",
            model=spam_model,
            top_k=None,
            truncation=True,
            max_length=512,
        )

        self.models_loaded = True
        status = "fine-tuned" if self._using_fine_tuned else "base (pre-trained)"
        print(f"[models] Ready — using {status} models.")

    # ------------------------------------------------------------------
    # Rule-based spam detection (always runs as first pass)
    # ------------------------------------------------------------------
    def _rule_based_spam(self, text: str) -> dict:
        stripped = text.strip()
        lower    = stripped.lower()
        words    = stripped.split()

        if len(words) < 3:
            return {"is_spam": True, "spam_score": 0.75, "spam_reason": "irrelevant"}

        if re.fullmatch(r"[\d\s\W]+", stripped):
            return {"is_spam": True, "spam_score": 0.80, "spam_reason": "irrelevant"}

        if re.search(r"(.)\1{4,}", stripped):
            return {"is_spam": True, "spam_score": 0.85, "spam_reason": "bot_pattern"}

        emoji_count = len(re.findall(
            r"[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF"
            r"\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF"
            r"\U00002702-\U000027B0\U000024C2-\U0001F251]+",
            stripped,
        ))
        if emoji_count > 5:
            return {"is_spam": True, "spam_score": 0.70, "spam_reason": "bot_pattern"}

        if len(stripped) < 30 and stripped.isupper() and len(words) <= 4:
            return {"is_spam": True, "spam_score": 0.75, "spam_reason": "bot_pattern"}

        promo_patterns = [
            r"check (out )?my (channel|video|page|content)",
            r"sub(scribe)?\s?(4|for)\s?sub",
            r"follow\s?me",
            r"https?://",
            r"www\.",
            r"\.(com|net|org|io)\b",
            r"free (followers|subscribers|views|likes)",
            r"make money",
            r"earn \$",
            r"(get|buy) (followers|views|subscribers)",
            r"dm\s?(me|for)",
            r"link\s?in\s?(bio|description)",
        ]
        for pat in promo_patterns:
            if re.search(pat, lower):
                return {"is_spam": True, "spam_score": 0.90, "spam_reason": "promotional"}

        return {"is_spam": False, "spam_score": 0.05, "spam_reason": "legitimate"}

    # ------------------------------------------------------------------
    # Core analysis
    # ------------------------------------------------------------------
    def analyze_comment(self, text: str) -> dict:
        if not self.models_loaded:
            self.load_models()

        truncated = text[:512]

        # Sentiment
        try:
            results         = self.sentiment_pipeline(truncated)[0]
            best            = max(results, key=lambda x: x["score"])
            sentiment_label = _SENTIMENT_LABEL_MAP.get(best["label"], "neutral")
            sentiment_score = round(float(best["score"]), 4)
        except Exception:
            sentiment_label = "neutral"
            sentiment_score = 0.5

        # Spam — rule-based first; fine-tuned ML model overrides when confident
        spam_result = self._rule_based_spam(text)

        if self._using_fine_tuned and self.spam_pipeline is not None:
            try:
                spam_preds = self.spam_pipeline(truncated)[0]
                for pred in spam_preds:
                    if pred["label"] in ("LABEL_1", "spam", "SPAM", "1") and pred["score"] > 0.75:
                        spam_result = {
                            "is_spam":    True,
                            "spam_score": round(float(pred["score"]), 4),
                            "spam_reason": "ml_detected",
                        }
                        break
            except Exception:
                pass

        return {
            "sentiment_label": sentiment_label,
            "sentiment_score": sentiment_score,
            "is_spam":         spam_result["is_spam"],
            "spam_score":      round(spam_result["spam_score"], 4),
            "spam_reason":     spam_result["spam_reason"],
        }

    def analyze_batch(self, comments: list[dict]) -> list[dict]:
        if not self.models_loaded:
            self.load_models()
        results = []
        for comment in comments:
            analysis = self.analyze_comment(comment.get("text", ""))
            results.append({**comment, **analysis})
        return results


# Module-level singleton — imported by main.py
analyzer = CommentAnalyzer()
