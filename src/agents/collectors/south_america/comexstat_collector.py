import requests
import json
from pathlib import Path
from datetime import datetime

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class ComexstatCollector:
    def __init__(self):
        self.base_url = "https://api-comexstat.mdic.gov.br"

    def collect(self, endpoint: str, params: dict, output_folder: str = "data"):
        url = f"{self.base_url}/{endpoint}"

        # TESTE: ignora verificação SSL (não use isso em produção)
        response = requests.get(url, params=params, verify=False)
        response.raise_for_status()

        data = response.json()

        Path(output_folder).mkdir(parents=True, exist_ok=True)

        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        file_path = Path(output_folder) / f"comexstat_{timestamp}.json"

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        print(f"Arquivo salvo em: {file_path}")
        return file_path
