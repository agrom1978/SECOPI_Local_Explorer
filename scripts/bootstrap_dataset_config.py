import os
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path



def _find_default_docx(root: Path) -> Path:
    candidates = list(root.glob("*Diccionario_de_Datos-SECOP_I*.docx"))
    if not candidates:
        candidates = list(root.parent.glob("*Diccionario_de_Datos-SECOP_I*.docx"))
    if not candidates:
        raise FileNotFoundError("Dictionary DOCX not found in project root.")
    if len(candidates) > 1:
        candidates.sort()
    return candidates[0]


def _parse_docx_rows(docx_path: Path):
    with zipfile.ZipFile(docx_path) as z:
        xml = z.read("word/document.xml")

    ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    root = ET.fromstring(xml)

    rows = []
    for tbl in root.findall(".//w:tbl", ns):
        for tr in tbl.findall(".//w:tr", ns):
            cells = []
            for tc in tr.findall(".//w:tc", ns):
                texts = []
                for t in tc.findall(".//w:t", ns):
                    texts.append(t.text or "")
                cell_text = "".join(texts).strip()
                cells.append(cell_text)
            if cells:
                rows.append(cells)
    return rows


def build_config(docx_path: Path):
    rows = _parse_docx_rows(docx_path)
    if not rows:
        raise ValueError("No rows found in dictionary DOCX.")

    data_rows = rows[1:]
    api_names = []
    for r in data_rows:
        if len(r) < 4:
            continue
        api = (r[3] or "").strip()
        if api:
            api_names.append(api)

    if not api_names:
        raise ValueError("No API field names found in dictionary DOCX.")

    fields = {api: api for api in api_names}
    select = api_names + [":updated_at"]

    return {
        "primary_key": "uid",
        "fields": fields,
        "system_fields": {
            "updated_at": ":updated_at",
        },
        "select": select,
    }


def _yaml_escape(value: str) -> str:
    if value == "" or any(ch in value for ch in [":", "#", "{", "}", "[", "]", ",", "&", "*", "?", "|", "-", "<", ">", "=", "!", "%", "@", "\\"]):
        return f"\"{value}\""
    return value


def _dump_yaml(cfg: dict) -> str:
    lines = []
    lines.append(f"primary_key: {_yaml_escape(cfg['primary_key'])}")

    lines.append("fields:")
    for k, v in cfg["fields"].items():
        lines.append(f"  {k}: {_yaml_escape(v)}")

    lines.append("system_fields:")
    for k, v in cfg["system_fields"].items():
        lines.append(f"  {k}: {_yaml_escape(v)}")

    lines.append("select:")
    for item in cfg["select"]:
        lines.append(f"  - {_yaml_escape(item)}")

    return "\n".join(lines) + "\n"


def main():
    root = Path(__file__).resolve().parents[1]
    docx_path = os.getenv("DOCX_PATH")
    if docx_path:
        docx = Path(docx_path)
    else:
        docx = _find_default_docx(root)

    out_path = os.getenv("OUT_YML", str(root / "config" / "dataset.yml"))
    out_file = Path(out_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)

    cfg = build_config(docx)
    with out_file.open("w", encoding="utf-8") as f:
        f.write(_dump_yaml(cfg))

    print(f"Wrote config to {out_file}")


if __name__ == "__main__":
    main()
