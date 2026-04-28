import os
import re
import sys
import requests
from dotenv import load_dotenv
from pathlib import Path

# Search common locations for the .env file (cwd, then repo root)
for env_path in [
    Path.cwd() / ".env",
    Path(__file__).resolve().parent.parent / ".env",
]:
    if env_path.exists():
        load_dotenv(env_path)
        break

PAT_NAME = os.getenv("TABLEAU_PAT_NAME")
PAT_SECRET = os.getenv("TABLEAU_PAT_SECRET")

JSON_HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}


def check(resp):
    if not resp.ok:
        raise RuntimeError(f"{resp.status_code} {resp.reason} at {resp.url}\nBody: {resp.text}")
    return resp


def parse_tableau_url(url):
    match = re.match(r"(https://[^/]+)/#/site/([^/]+)/(?:views|workbooks)/([^/?]+)", url)
    if not match:
        raise ValueError(f"Could not parse Tableau URL: {url}")
    server, site, workbook_name = match.groups()
    return server, site, workbook_name


def get_api_version(server):
    # serverinfo requires a version in the path; use a baseline and read back the supported version
    resp = check(
        requests.get(f"{server}/api/3.19/serverinfo", headers={"Accept": "application/json"})
    )
    return resp.json()["serverInfo"]["restApiVersion"]


def signin(server, api_version, site):
    if not PAT_NAME or not PAT_SECRET:
        raise RuntimeError("TABLEAU_PAT_NAME / TABLEAU_PAT_SECRET missing from .env")
    resp = check(
        requests.post(
            f"{server}/api/{api_version}/auth/signin",
            headers=JSON_HEADERS,
            json={
                "credentials": {
                    "personalAccessTokenName": PAT_NAME,
                    "personalAccessTokenSecret": PAT_SECRET,
                    "site": {"contentUrl": site},
                }
            },
        )
    )
    creds = resp.json()["credentials"]
    return creds["token"], creds["site"]["id"]


def get_workbook_id(server, api_version, site_id, token, workbook_slug):
    # URLs contain the workbook's contentUrl (slug), not its display name, so filter by that
    resp = check(
        requests.get(
            f"{server}/api/{api_version}/sites/{site_id}/workbooks",
            headers={"x-tableau-auth": token, "Accept": "application/json"},
            params={"filter": f"contentUrl:eq:{workbook_slug}"},
        )
    )
    workbooks = resp.json().get("workbooks", {}).get("workbook", [])
    if not workbooks:
        raise ValueError(f"Workbook with contentUrl '{workbook_slug}' not found on site")
    return workbooks[0]["id"]


def download_workbook(server, api_version, site_id, token, workbook_id, output_path):
    resp = requests.get(
        f"{server}/api/{api_version}/sites/{site_id}/workbooks/{workbook_id}/content",
        headers={"x-tableau-auth": token},
        params={"includeExtract": "false"},  # false = .twb, true = .twbx
        stream=True,
    )
    check(resp)
    with open(output_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)


def signout(server, api_version, token):
    requests.post(
        f"{server}/api/{api_version}/auth/signout",
        headers={"x-tableau-auth": token},
    )


def main(tableau_url, output_path=None):
    server, site, workbook_name = parse_tableau_url(tableau_url)

    if output_path is None:
        output_path = f"{workbook_name}.twb"

    print(f"Detecting API version...")
    api_version = get_api_version(server)
    print(f"Using API version {api_version}")

    print(f"Signing in to {server} (site: {site})...")
    token, site_id = signin(server, api_version, site)

    try:
        print(f"Finding workbook '{workbook_name}'...")
        workbook_id = get_workbook_id(server, api_version, site_id, token, workbook_name)

        print(f"Downloading to {output_path}...")
        download_workbook(server, api_version, site_id, token, workbook_id, output_path)
        print("Done.")
    finally:
        signout(server, api_version, token)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python download_workbook.py <tableau_url> [output.twb]")
        sys.exit(1)

    url = sys.argv[1]
    out = sys.argv[2] if len(sys.argv) > 2 else None
    main(url, out)
