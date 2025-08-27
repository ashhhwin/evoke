#file to check the versions of all the py packages
import importlib
import pkg_resources

# List of packages you care about
packages = [
    "openpyxl",
    "pandas",
    "pandas_market_calendars",
    "finnhub",
    "tqdm",
    "python-dotenv",
    "google-cloud-secret-manager",
    "requests",
    "gradio",
    "polars",
    "gradio-calendar",
    "plotly",
    "google-cloud-storage",
    "argparse",
    "flask",
    "gunicorn",
    "gcsfs",
    "pyarrow",
]

print("Installed package versions:\n")
for pkg in packages:
    try:
        version = pkg_resources.get_distribution(pkg).version
    except pkg_resources.DistributionNotFound:
        # Sometimes import name differs from pip name
        try:
            module = importlib.import_module(pkg.replace("-", "_"))
            version = getattr(module, "__version__", "unknown")
        except Exception:
            version = "not installed"
    print(f"{pkg}=={version}")
