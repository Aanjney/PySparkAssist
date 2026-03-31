import os

def test_settings_loads_defaults():
    os.environ["GROQ_API_KEY"] = "test_key"
    from pysparkassist.config import Settings
    s = Settings()
    assert s.groq_api_key == "test_key"
    assert isinstance(s.groq_model, str) and len(s.groq_model) > 0
    assert s.relevance_threshold == 0.35
    assert s.rate_limit_rpm == 20
