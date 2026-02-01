resource "superset_dashboard" "example" {
  dashboard_title = "Sales Overview"
  slug            = "sales-overview"
  published       = true
}

# Dashboard with custom CSS
resource "superset_dashboard" "styled" {
  dashboard_title = "Executive Dashboard"
  slug            = "exec-dashboard"
  published       = true
  css             = <<-EOF
    .dashboard-header {
      background-color: #1a1a2e;
    }
    .chart-container {
      border-radius: 8px;
    }
  EOF
}

# Dashboard with certification
resource "superset_dashboard" "certified" {
  dashboard_title       = "Financial Reports"
  slug                  = "financial-reports"
  published             = true
  certified_by          = "Finance Team"
  certification_details = "Approved for Q4 2024 reporting"
}

# Minimal dashboard (required fields only)
resource "superset_dashboard" "minimal" {
  dashboard_title = "My Dashboard"
}

