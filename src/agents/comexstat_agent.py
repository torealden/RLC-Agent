import argparse
from src.agents.collectors.south_america.comexstat_collector import ComexstatCollector


def main() -> None:
    parser = argparse.ArgumentParser(description="COMEXSTAT collector (MVP)")
    parser.add_argument("--endpoint", required=True, help="Endpoint da API (ex: api/...)")
    parser.add_argument("--out", default="data/comexstat", help="Pasta de saída")
    parser.add_argument("--param", action="append", default=[], help="Parâmetro key=value (pode repetir)")

    args = parser.parse_args()

    params = {}
    for kv in args.param:
        if "=" not in kv:
            raise SystemExit(f"Param inválido: {kv} (use key=value)")
        k, v = kv.split("=", 1)
        params[k] = v

    collector = ComexstatCollector()
    collector.collect(endpoint=args.endpoint, params=params, output_folder=args.out)


if __name__ == "__main__":
    main()
