class BreweriesPackage:

    BASE_URL = "https://api.openbrewerydb.org/v1/breweries?page={}&per_page={}"

    BREWERIES_HEADERS = {
        "authority": "api.openbrewerydb.org",
        "method": "GET",
        "scheme": "https",
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9,pt-BR;q=0.8,pt;q=0.7,es-US;q=0.6,es;q=0.5",
        "cache-control": "no-cache",
        "origin": "https://www.openbrewerydb.org",
        "pragma": "no-cache",
        "priority": "u=1, i",
        "referer": "https://www.openbrewerydb.org/",
        "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    }
