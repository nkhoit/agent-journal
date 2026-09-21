"""Real Chromium acceptance against the shared-viewer Axum HTTP stack."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
from urllib.parse import urlsplit

from playwright.sync_api import sync_playwright


def main():
    binary = Path("target/debug/examples/web_fixture" + (".exe" if os.name == "nt" else ""))
    with tempfile.TemporaryDirectory(prefix="journal-web-") as temporary:
        process = subprocess.Popen(
            [str(binary.resolve()), str(Path(temporary) / "journal.db")],
            stdout=subprocess.PIPE, text=True,
        )
        try:
            fixture = json.loads(process.stdout.readline())
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch()
                context = browser.new_context()
                page = context.new_page()
                requests = []
                dialogs = []
                page.on("request", lambda request: requests.append(request.url))
                page.on("dialog", lambda dialog: (dialogs.append(dialog.message), dialog.dismiss()))
                record = "/web/records/" + fixture["record"]
                for path in [
                    "/web", "/web/spaces/space", record, record + "/thread",
                    "/web/spaces/space/search?q=malicioussnippet",
                ]:
                    response = page.goto(fixture["viewer"] + path)
                    assert response.status == 200, path
                    headers = response.all_headers()
                    assert headers["cache-control"] == "no-store"
                    assert headers["referrer-policy"] == "no-referrer"
                    assert "script-src 'none'" in headers["content-security-policy"]
                    assert "frame-ancestors 'none'" in headers["content-security-policy"]
                    assert page.locator("script,img,svg,iframe,object,embed,style").count() == 0
                    assert page.locator("[onload],[onerror],[onclick]").count() == 0
                    for href in page.locator("a").evaluate_all("(nodes) => nodes.map(n => n.getAttribute('href'))"):
                        assert href.startswith("/web") or href.startswith("https://"), href
                    assert page.evaluate("globalThis.compromised === undefined")
                    assert page.evaluate("localStorage.length === 0 && sessionStorage.length === 0")
                    assert context.cookies() == []
                assert not dialogs
                assert all(urlsplit(url).netloc == urlsplit(fixture["viewer"]).netloc for url in requests)
                page.goto(fixture["viewer"] + record)
                author = fixture["author"]
                assert page.locator("article > header").inner_text().count(
                    f"Authenticated author: {author}"
                ) == 1
                assert "forged-envelope" in page.locator("fieldset").inner_text()
                assert page.locator("fieldset h1,fieldset h2,fieldset form").count() == 0
                assert page.locator("fieldset strong").filter(has_text="ordinary Markdown").count() == 1
                # The CSP must block active elements even if future rendering regresses.
                page.evaluate("""() => {
                    const script = document.createElement('script');
                    script.textContent = 'globalThis.compromised = true';
                    document.body.appendChild(script);
                }""")
                assert page.evaluate("globalThis.compromised === undefined")
                page.goto(fixture["viewer"] + "/web/spaces/space?limit=1")
                assert page.locator("article").count() == 1
                page.locator("a[rel=next]").click()
                assert "Fixture reply" in page.locator("article").inner_text()
                page.goto(fixture["viewer"] + "/web/spaces/space")
                page.locator("input[name=q]").fill("malicioussnippet")
                page.locator("button[type=submit]").click()
                assert page.locator("article").count() == 1
                assert "<img" in page.locator("article pre").inner_text()
                assert page.locator("article img").count() == 0
                for viewer, status in [("viewer", 200), ("recipient", 200), ("reader", 404), ("outsider", 404)]:
                    response = page.goto(fixture[viewer] + record + "/delivery-status")
                    assert response.status == status
                    if status == 200:
                        assert page.locator("tbody tr").count() == 1
                        assert page.locator("tbody").inner_text().startswith(fixture["recipient_id"])
                        assert page.locator("h1").inner_text() == "Receipt status"
                        assert "unacknowledged" in page.locator("tbody").inner_text()
                        assert "Attempts" not in page.locator("thead").inner_text()
                        assert "host-accepted" not in page.locator("tbody").inner_text()
                    else:
                        assert "recipient" not in page.content()
                for path in [record, record + "/thread", "/web/spaces/space", "/web/spaces/space/search?q=malicioussnippet"]:
                    assert page.goto(fixture["outsider"] + path).status == 200
                assert page.goto(fixture["api"] + record).status == 404
                assert context.request.post(fixture["viewer"] + record).status == 405
                assert context.request.get(fixture["viewer"] + "/v1/spaces").status == 404
                assert context.request.get(fixture["viewer"] + "/v1/admin/metrics").status == 404
                assert context.request.post(
                    fixture["api"] + "/v1/spaces/space/records",
                    data={"kind": "note", "content": "must not publish"},
                ).status == 401
                browser.close()
            print("Browser security: Chromium rendering, CSP, navigation, public policy, delivery and listener isolation passed")
        finally:
            process.terminate()
            process.wait(timeout=15)


if __name__ == "__main__":
    main()
