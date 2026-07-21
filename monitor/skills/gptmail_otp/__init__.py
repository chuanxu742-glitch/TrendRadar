__all__ = ["GPTMailClient", "PublicKeyProvider"]


def __getattr__(name: str):
    if name == "GPTMailClient":
        from .client import GPTMailClient

        return GPTMailClient
    if name == "PublicKeyProvider":
        from .provider import PublicKeyProvider

        return PublicKeyProvider
    raise AttributeError(name)
