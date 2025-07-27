# 🌐 IMScraper Web

**IMScraper Web** is the web version of the IMScraper project, a tool designed to fetch SEO-related metrics from a large number of websites using trusted, real-world data sources like:

- **Ahrefs**
- **DataForSEO**
- **Majestic**

It provides a simple web interface designed for easy hosting and low resource usage with a background processor that automates bulk metric collection, without the need for local software on end user machines. Ideal for SEO professionals, agencies, researchers, and analysts.

---

## Features

- Fetch key metrics from supported APIs, including **traffic**, **domain rating**, **referring domains**, **rank**, **HTTPS availability**, and **trust flow**.
- Integrates with leading APIs: **Ahrefs**, **DataForSEO**, and **Majestic**. Requests can be customized to include all data points.
- Generates a clean `.ods` spreadsheet with all results.
- User-friendly web interface for submitting and tracking jobs, allowing users to close out of the website and check on progress later.
- Asynchronous background processing (no blocking)
- Secure job handling with unique job IDs

---

## Project Structure

```
imscraper-web/
│
├── app.py                # Flask web interface
├── background_worker.py  # Asynchronous job processor
├── requirements.txt      # Python dependencies
│
├── templates/
│   └── index.html        # Web UI for submitting jobs
├── jobs/                 # Incoming job queue (auto-created)
├── results/              # Completed spreadsheets (auto-created)
└── logs/                 # Logs for debugging (auto-created)
```

---

## Output Format

After processing, the tool generates a standard `.ods` spreadsheet that includes columns such as:

- URL
- Status Code
- HTTPS Availability
- Majestic Topics
- Ahrefs Domain Rating / Referring Domains / Traffic
- DataForSEO Rank / Traffic / Referring Domains

Example output:

| URL            | HTTPS | Ahrefs DR | Ref Domains | Traffic |
|----------------|--------|-----------|-------------|---------|
| example.com    | Yes    | 72        | 150         | 34,000  |
| somesite.net   | No     | 50        | 84          | 12,300  |

---

## API Credentials

To use this tool, you’ll need valid credentials from:
- [Ahrefs](https://ahrefs.com/api)
- [DataForSEO](https://dataforseo.com/)
- [Majestic](https://majestic.com/support/api)

> API keys are entered via the web interface and **never stored permanently**. Premium subscriptions from these third parties may be required for most data points.

---

## Acknowledgements

This project uses the following open-source libraries:

- [Flask](https://palletsprojects.com/p/flask/) – BSD License
- [Flask-CORS](https://github.com/corydolphin/flask-cors) – MIT License
- [aiohttp](https://docs.aiohttp.org/) – Apache 2.0 License
- [odfpy](https://github.com/eea/odfpy) – Apache 2.0 License

---

## License

This project is licensed under the MIT License. See the [LICENSE](./LICENSE) file for details.

---
