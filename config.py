# config.py

CLASSIFICATION_KEYWORDS = {
    "Technical Issue": ["bug", "error", "crash", "technical", "glitch", "failure"],
    "Billing Inquiry": ["bill", "invoice", "charge", "payment", "money", "subscription"],
    "Service Quality": ["slow", "unresponsive", "poor quality", "bad service", "intermittent"],
    "Feature Request": ["wish", "suggest", "add feature", "new functionality"],
    "General Inquiry": ["question", "help", "information"]
}

# You can add more configuration variables here, e.g., database connection strings etc.
# For now, this is enough for the keyword-driven classifier.