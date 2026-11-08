# Configuration of the Superset 3.1.3 review instance. TAGGING_SYSTEM stays off: with it on,
# Superset 3.1 fails to delete a chart or a dashboard whenever another object has the same id.
FAB_ADD_SECURITY_API = True
FEATURE_FLAGS = {
    "EMBEDDED_SUPERSET": True,
    "SSH_TUNNELING": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
    "ALERT_REPORTS": True,
}
