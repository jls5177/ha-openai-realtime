import asyncio
from types import SimpleNamespace

import app.web_search_tool as web_search


def test_web_search_uses_location_only_when_set_and_fallbacks_are_english(monkeypatch):
    async def scenario():
        inputs = []
        answers = []

        async def create(**kwargs):
            inputs.append(kwargs["input"])
            return SimpleNamespace(output_text="")

        monkeypatch.setattr(
            web_search, "AsyncOpenAI",
            lambda **kwargs: SimpleNamespace(responses=SimpleNamespace(create=create)),
        )

        async def result_callback(answer):
            answers.append(answer)

        params = SimpleNamespace(arguments={"query": "What's the weather?"}, result_callback=result_callback)
        await web_search.create_web_search_tool_handler("key", "model", "Temple, Texas")(params)
        await web_search.create_web_search_tool_handler("key", "model")(params)
        assert "assume it's about Temple, Texas" in inputs[0]
        assert "assume it's about" not in inputs[1]
        assert answers == ["I couldn't find anything about that online."] * 2
        params.arguments = {}
        await web_search.create_web_search_tool_handler("key", "model")(params)
        assert answers[-1] == "No search query received."

    asyncio.run(scenario())
