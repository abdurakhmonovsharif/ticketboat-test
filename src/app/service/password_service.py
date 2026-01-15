import secrets
import string


class PasswordGenerator:
    def __init__(self, length: int = 12):
        self.length = length
        self.alphabet = (
                string.ascii_uppercase +
                string.ascii_lowercase +
                string.digits +
                string.punctuation
        )

    def generate(self) -> str:
        """Generate a secure random password."""
        password = [
            secrets.choice(string.ascii_uppercase),
            secrets.choice(string.ascii_lowercase),
            secrets.choice(string.digits),
            secrets.choice(string.punctuation),
        ]
        password += [secrets.choice(self.alphabet) for _ in range(self.length - 4)]
        secrets.SystemRandom().shuffle(password)
        return ''.join(password)
