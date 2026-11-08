# Configuration of the Superset 6.1.0 review instance.
FAB_ADD_SECURITY_API = True
FEATURE_FLAGS = {
    "EMBEDDED_SUPERSET": True,
    "SSH_TUNNELING": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
    "ALERT_REPORTS": True,
    "TAGGING_SYSTEM": True,
}
