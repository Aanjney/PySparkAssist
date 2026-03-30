import os

def test_settings_loads_defaults():
    os.environ["GROQ_API_KEY"] = "test_key"
    from pysparkassist.config import Settings
    s = Settings()
    assert s.groq_api_key == "test_key"
    assert s.groq_model == "llama-3.3-70b-versatile"
    assert s.relevance_threshold == 0.35
    assert s.rate_limit_rpm == 20
