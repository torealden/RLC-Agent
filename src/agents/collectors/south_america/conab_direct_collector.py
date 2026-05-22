"""
CONAB Direct-Download Collector

Hits 6 stable text endpoints at
    https://portaldeinformacoes.conab.gov.br/downloads/arquivos/

| File                        | Bronze target                       | Cadence  |
|-----------------------------|-------------------------------------|----------|
| Frete.txt                   | bronze.conab_freight                | Monthly  |
| PrecoMinimo.txt             | bronze.conab_min_prices             | As-rev   |
| CustoProducao.txt           | bronze.conab_production_cost        | Monthly  |
| OfertaDemanda.txt           | bronze.conab_supply_demand_v2       | Monthly  |
| Estoques.txt                | bronze.conab_stocks                 | Monthly  |
| ArmazensCadastrados.txt     | bronze.conab_warehouses             | Quarterly|

Notes
- Encoding: ISO-8859-1 (Latin-1). The files are semicolon-delimited.
- Brazilian decimal format: comma as decimal separator.
- Trailing/leading whitespace pads many cells (fixed-display formatting).
- Estoques.txt is sometimes gzipped (~1 KB) and sometimes plain — handled either way.

Replaces the copy-paste-from-portal workflow.
"""

from __future__ import annotations

import gzip
import io
import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import requests
import urllib3

from .base_collector import (
    BaseCollector,
    CollectorConfig,
    CollectorResult,
    DataFrequency,
    AuthType,
)
from src.services.database.db_config import get_connection

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)


CONAB_BASE = "https://portaldeinformacoes.conab.gov.br/downloads/arquivos/"

ENDPOINTS: Dict[str, str] = {
    "freight":          "Frete.txt",
    "min_prices":       "PrecoMinimo.txt",
    "production_cost":  "CustoProducao.txt",
    "supply_demand":    "OfertaDemanda.txt",
    "stocks":           "Estoques.txt",
    "warehouses":       "ArmazensCadastrados.txt",
}


# -----------------------------------------------------------------------------
# Parsing helpers
# -----------------------------------------------------------------------------

def _strip(val: str) -> Optional[str]:
    v = (val or "").strip()
    return v if v else None


def _to_int(val: str) -> Optional[int]:
    v = _strip(val)
    if v is None:
        return None
    try:
        return int(float(v.replace(",", ".")))
    except (ValueError, TypeError):
        return None


def _to_float(val: str) -> Optional[float]:
    """Brazilian decimal (comma) → float."""
    v = _strip(val)
    if v is None or v.upper() in ("NI", "N/A", "-"):
        return None
    try:
        return float(v.replace(".", "").replace(",", ".")) if v.count(",") == 1 and v.count(".") >= 1 else float(v.replace(",", "."))
    except (ValueError, TypeError):
        return None


def _fetch_text(filename: str, timeout: int = 90) -> str:
    """Fetch a text file. Handles plain text or gzip transparently."""
    url = CONAB_BASE + filename
    r = requests.get(url, timeout=timeout, verify=False)
    r.raise_for_status()
    body = r.content
    # Detect gzip magic bytes
    if body[:2] == b"\x1f\x8b":
        body = gzip.decompress(body)
    # Latin-1 decoding (Brazilian Portuguese accents)
    return body.decode("iso-8859-1", errors="replace")


def _parse_semicolon(text: str) -> List[Dict[str, str]]:
    """Parse a semicolon-delimited file with header row. Returns list of dicts (raw strings)."""
    lines = text.splitlines()
    if not lines:
        return []
    headers = [h.strip() for h in lines[0].split(";")]
    rows: List[Dict[str, str]] = []
    for line in lines[1:]:
        if not line.strip():
            continue
        parts = line.split(";")
        # Pad short rows with empty strings (some files have ragged trailing fields)
        while len(parts) < len(headers):
            parts.append("")
        rows.append(dict(zip(headers, parts)))
    return rows


# -----------------------------------------------------------------------------
# Per-endpoint transformers
# -----------------------------------------------------------------------------

def transform_freight(rows: List[Dict[str, str]]) -> List[tuple]:
    """Frete.txt → bronze.conab_freight tuples."""
    out = []
    for r in rows:
        out.append((
            _strip(r.get("dsc_fonte")),
            _strip(r.get("municipio_origem")),
            _strip(r.get("cod_ibge_origem")),
            _strip(r.get("uf_origem")),
            _strip(r.get("municipio_destino")),
            _strip(r.get("cod_ibge_destino")),
            _strip(r.get("uf_destino")),
            _to_int(r.get("ano")),
            _to_int(r.get("mes")),
            _to_float(r.get("distancia_km")),
            _to_float(r.get("valor_frete_tonelada")),
            _to_float(r.get("valor_tonelada_km")),
        ))
    return out


def transform_min_prices(rows: List[Dict[str, str]]) -> List[tuple]:
    out = []
    for r in rows:
        out.append((
            _strip(r.get("descricao_produto_preco_minimo")),
            _to_int(r.get("id_produto")),
            _strip(r.get("uf")),
            _strip(r.get("regionalizacao")),
            _to_int(r.get("ano_inicio_vigencia")),
            _to_int(r.get("mes_incio_vigencia") or r.get("mes_inicio_vigencia")),
            _to_int(r.get("ano_termino_vigencia")),
            _to_int(r.get("mes_termino_vigencia")),
            _to_float(r.get("preco")),
            _strip(r.get("dsc_unidade_comercializacao")),
            _strip(r.get("nome_normativo")),
            _strip(r.get("url")),
        ))
    return out


def transform_production_cost(rows: List[Dict[str, str]]) -> List[tuple]:
    out = []
    for r in rows:
        out.append((
            _strip(r.get("empreendimento")),
            _to_int(r.get("ano")),
            _to_int(r.get("mes")),
            _to_int(r.get("ano_mes")),
            _strip(r.get("produto")),
            _to_int(r.get("id_produto")),
            _strip(r.get("safra")),
            _strip(r.get("uf")),
            _strip(r.get("municipio")),
            _strip(r.get("cod_ibge")),
            _strip(r.get("unidade_comercializacao")),
            _to_float(r.get("vlr_custo_variavel_ha")),
            _to_float(r.get("vlr_custo_variavel_unidade")),
            _to_float(r.get("vlr_custo_fixo_ha")),
            _to_float(r.get("vlr_custo_fixo_unidade")),
            _to_float(r.get("vlr_renda_fator_ha")),
            _to_float(r.get("vlr_renda_fator_unidade")),
        ))
    return out


def transform_supply_demand(rows: List[Dict[str, str]]) -> List[tuple]:
    out = []
    for r in rows:
        out.append((
            _strip(r.get("produto")),
            _to_int(r.get("id_produto")),
            _strip(r.get("dsc_safra")),
            _to_float(r.get("estoque_inicial_1000t")),
            _to_float(r.get("producao_1000t")),
            _to_float(r.get("importacao_1000t")),
            _to_float(r.get("consumo_1000t")),
            _to_float(r.get("exportacao_1000t")),
            _to_float(r.get("estoque_final_1000t")),
        ))
    return out


def transform_stocks(rows: List[Dict[str, str]]) -> List[tuple]:
    out = []
    for r in rows:
        out.append((
            _strip(r.get("produto")),
            _to_int(r.get("id_produto")),
            _strip(r.get("nom_municipio")),
            _strip(r.get("cod_ibge")),
            _strip(r.get("uf")),
            _to_int(r.get("num_ano")),
            _to_int(r.get("num_mes")),
            _strip(r.get("conta_operacional")),
            _to_float(r.get("qtd_estoque_kg")),
        ))
    return out


def transform_warehouses(rows: List[Dict[str, str]]) -> List[tuple]:
    out = []
    for r in rows:
        out.append((
            _strip(r.get("identificacao_armazem")),
            _strip(r.get("dsc_especie_armazem")),
            _strip(r.get("dsc_tipo_armazem")),
            _strip(r.get("dsc_tipo_entidade")),
            _strip(r.get("dsc_tipo_pessoa")),
            _strip(r.get("nom_municipio")),
            _strip(r.get("cod_ibge")),
            _strip(r.get("uf")),
            _to_float(r.get("qtd_capacidade_estatica(t)") or r.get("qtd_capacidade_estatica_t")),
            _to_float(r.get("qtd_capacidade_expedicao(t)") or r.get("qtd_capacidade_expedicao_t")),
            _to_float(r.get("qtd_capacidade_recepcao(t)") or r.get("qtd_capacidade_recepcao_t")),
            _to_float(r.get("latitude")),
            _to_float(r.get("longitude")),
            _strip(r.get("nome_armazenador")),
            _strip(r.get("endereco")),
            _strip(r.get("email")),
        ))
    return out


# -----------------------------------------------------------------------------
# Bronze persistence — TRUNCATE+RELOAD per endpoint (idempotent monthly refresh)
# -----------------------------------------------------------------------------

PERSIST_SPECS = {
    "freight": {
        "table": "bronze.conab_freight",
        "columns": (
            "dsc_fonte, municipio_origem, cod_ibge_origem, uf_origem, "
            "municipio_destino, cod_ibge_destino, uf_destino, "
            "ano, mes, distancia_km, valor_frete_tonelada, valor_tonelada_km"
        ),
        "placeholders": "(" + ",".join(["%s"] * 12) + ")",
        "transform": transform_freight,
    },
    "min_prices": {
        "table": "bronze.conab_min_prices",
        "columns": (
            "descricao_produto, id_produto, uf, regionalizacao, "
            "ano_inicio_vigencia, mes_inicio_vigencia, "
            "ano_termino_vigencia, mes_termino_vigencia, "
            "preco, dsc_unidade_comercializacao, nome_normativo, url"
        ),
        "placeholders": "(" + ",".join(["%s"] * 12) + ")",
        "transform": transform_min_prices,
    },
    "production_cost": {
        "table": "bronze.conab_production_cost",
        "columns": (
            "empreendimento, ano, mes, ano_mes, produto, id_produto, safra, "
            "uf, municipio, cod_ibge, unidade_comercializacao, "
            "vlr_custo_variavel_ha, vlr_custo_variavel_unidade, "
            "vlr_custo_fixo_ha, vlr_custo_fixo_unidade, "
            "vlr_renda_fator_ha, vlr_renda_fator_unidade"
        ),
        "placeholders": "(" + ",".join(["%s"] * 17) + ")",
        "transform": transform_production_cost,
    },
    "supply_demand": {
        "table": "bronze.conab_supply_demand_v2",
        "columns": (
            "produto, id_produto, dsc_safra, "
            "estoque_inicial_1000t, producao_1000t, importacao_1000t, "
            "consumo_1000t, exportacao_1000t, estoque_final_1000t"
        ),
        "placeholders": "(" + ",".join(["%s"] * 9) + ")",
        "transform": transform_supply_demand,
    },
    "stocks": {
        "table": "bronze.conab_stocks",
        "columns": (
            "produto, id_produto, nom_municipio, cod_ibge, uf, "
            "num_ano, num_mes, conta_operacional, qtd_estoque_kg"
        ),
        "placeholders": "(" + ",".join(["%s"] * 9) + ")",
        "transform": transform_stocks,
    },
    "warehouses": {
        "table": "bronze.conab_warehouses",
        "columns": (
            "identificacao_armazem, dsc_especie_armazem, dsc_tipo_armazem, "
            "dsc_tipo_entidade, dsc_tipo_pessoa, nom_municipio, cod_ibge, uf, "
            "qtd_capacidade_estatica_t, qtd_capacidade_expedicao_t, qtd_capacidade_recepcao_t, "
            "latitude, longitude, nome_armazenador, endereco, email"
        ),
        "placeholders": "(" + ",".join(["%s"] * 16) + ")",
        "transform": transform_warehouses,
    },
}


# -----------------------------------------------------------------------------
# Config + collector
# -----------------------------------------------------------------------------

@dataclass
class CONABDirectConfig(CollectorConfig):
    source_name: str = "CONAB_DIRECT"
    source_url: str = CONAB_BASE
    auth_type: AuthType = AuthType.NONE
    frequency: DataFrequency = DataFrequency.MONTHLY
    rate_limit_per_minute: int = 10
    timeout: int = 90
    endpoints: List[str] = field(default_factory=lambda: list(ENDPOINTS.keys()))


class CONABDirectCollector(BaseCollector):
    """
    Direct-download CONAB collector — replaces the copy-paste portal workflow.

    Each .collect() run pulls all 6 files (or the subset listed in config.endpoints),
    TRUNCATE+RELOADS the corresponding bronze table. Idempotent.
    """

    def __init__(self, config: Optional[CONABDirectConfig] = None):
        config = config or CONABDirectConfig()
        super().__init__(config)
        self.config: CONABDirectConfig = config

    def get_table_name(self) -> str:
        return "conab_direct_multi"

    def parse_response(self, raw_data: Any, **kwargs) -> Any:
        """Not used — fetch_data parses inline per endpoint."""
        return raw_data

    def fetch_data(self, **kwargs) -> CollectorResult:
        """Fetch all endpoints, return raw row counts in result.data."""
        endpoints_to_run = kwargs.get("endpoints") or self.config.endpoints
        summary: Dict[str, int] = {}
        errors: Dict[str, str] = {}
        per_endpoint_rows: Dict[str, List[tuple]] = {}

        for key in endpoints_to_run:
            if key not in ENDPOINTS:
                errors[key] = f"unknown endpoint key '{key}'"
                continue
            filename = ENDPOINTS[key]
            try:
                logger.info(f"[CONAB-direct] fetching {filename}")
                text = _fetch_text(filename, timeout=self.config.timeout)
                rows = _parse_semicolon(text)
                tuples = PERSIST_SPECS[key]["transform"](rows)
                per_endpoint_rows[key] = tuples
                summary[key] = len(tuples)
                logger.info(f"[CONAB-direct] parsed {len(tuples)} rows from {filename}")
            except Exception as exc:
                logger.error(f"[CONAB-direct] {filename} failed: {exc}")
                errors[key] = str(exc)
                summary[key] = 0

        success = bool(per_endpoint_rows) and not errors
        return CollectorResult(
            success=success,
            source=self.config.source_name,
            collected_at=datetime.utcnow(),
            records_fetched=sum(summary.values()),
            data={
                "summary": summary,
                "rows": per_endpoint_rows,
                "errors": errors,
                "endpoints_attempted": endpoints_to_run,
            },
            warnings=[f"{k}: {v}" for k, v in errors.items()],
        )

    def save_to_bronze(self, result: CollectorResult) -> int:
        """
        TRUNCATE+RELOAD each endpoint's bronze table.
        Returns total rows inserted across all endpoints.
        """
        if not result or not result.data:
            return 0
        per_endpoint = result.data.get("rows", {})
        total_inserted = 0

        with get_connection() as conn:
            with conn.cursor() as cur:
                for key, tuples in per_endpoint.items():
                    if not tuples:
                        continue
                    spec = PERSIST_SPECS[key]
                    cur.execute(f"TRUNCATE TABLE {spec['table']}")
                    # batch insert
                    args_str = ",".join(
                        cur.mogrify(spec["placeholders"], t).decode("utf-8") for t in tuples
                    )
                    cur.execute(
                        f"INSERT INTO {spec['table']} ({spec['columns']}) VALUES {args_str}"
                    )
                    inserted = cur.rowcount
                    logger.info(f"[CONAB-direct] inserted {inserted} into {spec['table']}")
                    total_inserted += inserted
            conn.commit()
        return total_inserted

    def collect(self, **kwargs) -> CollectorResult:
        """BaseCollector hook — fetch + save."""
        result = self.fetch_data(**kwargs)
        if result.success or result.records_fetched > 0:
            try:
                inserted = self.save_to_bronze(result)
                result.data["rows_persisted"] = inserted
            except Exception as exc:
                logger.error(f"[CONAB-direct] persist failed: {exc}")
                result.success = False
                result.error_message = f"persist failed: {exc}"
        return result


__all__ = ["CONABDirectCollector", "CONABDirectConfig", "ENDPOINTS"]
