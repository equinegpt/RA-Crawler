# api/track_types.py

from __future__ import annotations

# -------- Aliases -> Canonical track name (UPPERCASE) ----------
_ALIAS_EQUIV = {
    # VIC
    ("VIC", "SANDOWN"): "SANDOWN",
    ("VIC", "SANDOWN HILLSIDE"): "SANDOWN HILLSIDE",
    ("VIC", "SANDOWN LAKESIDE"): "SANDOWN LAKESIDE",
    ("VIC", "WERRIBEE PARK"): "WERRIBEE",
    ("VIC", "CRANBOURNE TRN"): "CRANBOURNE",
    ("VIC", "SOUTHSIDE CRANBOURNE"): "CRANBOURNE",

    # NSW
    ("NSW", "RANDWICK KENSINGTON"): "KENSINGTON",
    ("NSW", "CANTERBURY PARK"): "CANTERBURY",
    ("NSW", "ROSEHILL"): "ROSEHILL GARDENS",

    # QLD
    ("QLD", "CALOUNDRA"): "SUNSHINE COAST",
    ("QLD", "SUNSHINE COAST INNER"): "SUNSHINE COAST",
    ("QLD", "AQUIS PARK GOLD COAST"): "GOLD COAST",

    # TAS
    ("TAS", "DEVONPORT TAPETA"): "DEVONPORT",
    ("TAS", "SPREYTON"): "DEVONPORT",

    # SA
    ("SA", "MURRAY BRIDGE GH"): "MURRAY BRIDGE",
    ("SA", "MURRAY BRIDGE PARK"): "MURRAY BRIDGE",
}

def canonical_track(state: str, track: str) -> str:
    s = (state or "").strip().upper()
    t = (track or "").strip().upper()
    return _ALIAS_EQUIV.get((s, t), t)

# -------- Grade table (STATE, TRACK) -> "M" | "P" | "C" ----------
TRACK_GRADE = {
    # ACT
    ("ACT", "CANBERRA"): "P",

    # NSW — metro
    ("NSW", "RANDWICK"): "M",
    ("NSW", "KENSINGTON"): "M",
    ("NSW", "ROSEHILL GARDENS"): "M",
    ("NSW", "CANTERBURY"): "M",
    ("NSW", "WARWICK FARM"): "M",
    # NSW — provincial
    ("NSW", "GOSFORD"): "P",
    ("NSW", "HAWKESBURY"): "P",
    ("NSW", "KEMBLA GRANGE"): "P",
    ("NSW", "NEWCASTLE"): "P",
    ("NSW", "WYONG"): "P",
    # NSW — country (examples)
    ("NSW", "NOWRA"): "C",
    ("NSW", "COFFS HARBOUR"): "C",
    ("NSW", "MUSWELLBROOK"): "C",
    ("NSW", "DUBBO"): "C",
    ("NSW", "TAREE"): "C",
    ("NSW", "SCONE"): "C",
    ("NSW", "GRAFTON"): "C",
    ("NSW", "TAMWORTH"): "C",

    # VIC — metro
    ("VIC", "FLEMINGTON"): "M",
    ("VIC", "CAULFIELD"): "M",
    ("VIC", "MOONEE VALLEY"): "M",
    ("VIC", "SANDOWN"): "M",
    ("VIC", "SANDOWN HILLSIDE"): "M",
    ("VIC", "SANDOWN LAKESIDE"): "M",
    # VIC — provincial
    ("VIC", "BALLARAT"): "P",
    ("VIC", "BENDIGO"): "P",
    ("VIC", "CRANBOURNE"): "P",
    ("VIC", "GEELONG"): "P",
    ("VIC", "KILMORE"): "P",
    ("VIC", "KYNETON"): "P",
    ("VIC", "MOE"): "P",
    ("VIC", "MORNINGTON"): "P",
    ("VIC", "PAKENHAM"): "P",
    ("VIC", "SALE"): "P",
    ("VIC", "SEYMOUR"): "P",
    ("VIC", "WARRNAMBOOL"): "P",
    ("VIC", "WERRIBEE"): "P",
    ("VIC", "YARRA GLEN"): "P",
    ("VIC", "TRARALGON"): "P",
    # VIC — country (examples)
    ("VIC", "WANGARATTA"): "C",
    ("VIC", "WODONGA"): "C",
    ("VIC", "SWAN HILL"): "C",
    ("VIC", "STAWELL"): "C",
    ("VIC", "TERANG"): "C",
    ("VIC", "ECHUCA"): "C",

    # QLD — metro
    ("QLD", "DOOMBEN"): "M",
    ("QLD", "EAGLE FARM"): "M",
    # QLD — provincial
    ("QLD", "SUNSHINE COAST"): "P",
    ("QLD", "IPSWICH"): "P",
    ("QLD", "TOOWOOMBA"): "P",
    ("QLD", "GOLD COAST"): "P",
    # QLD — country (examples)
    ("QLD", "TOWNSVILLE"): "C",
    ("QLD", "ROCKHAMPTON"): "C",
    ("QLD", "MACKAY"): "C",
    ("QLD", "CAIRNS"): "C",
    ("QLD", "ROMA"): "C",

    # SA
    ("SA", "MORPHETTVILLE"): "M",
    ("SA", "MORPHETTVILLE PARKS"): "M",
    ("SA", "BALAKLAVA"): "P",
    ("SA", "GAWLER"): "P",
    ("SA", "MURRAY BRIDGE"): "P",
    ("SA", "STRATHALBYN"): "P",
    ("SA", "PORT LINCOLN"): "C",
    ("SA", "NARACOORTE"): "C",
    ("SA", "PENOLA"): "C",
    ("SA", "MOUNT GAMBIER"): "C",

    # WA
    ("WA", "ASCOT"): "M",
    ("WA", "BELMONT"): "M",
    ("WA", "PINJARRA"): "P",
    ("WA", "NORTHAM"): "P",
    ("WA", "BUNBURY"): "P",
    ("WA", "YORK"): "P",
    ("WA", "KALGOORLIE"): "C",
    ("WA", "ESPERANCE"): "C",
    ("WA", "ALBANY"): "C",
    ("WA", "GERALDTON"): "C",

    # TAS
    ("TAS", "HOBART"): "M",
    ("TAS", "LAUNCESTON"): "M",
    ("TAS", "DEVONPORT"): "P",

    # NT
    ("NT", "DARWIN"): "M",
    ("NT", "ALICE SPRINGS"): "C",
    ("NT", "KATHERINE"): "C",
}

def infer_type(state: str, track: str) -> str | None:
    s = (state or "").strip().upper()
    t = canonical_track(s, track)
    return TRACK_GRADE.get((s, t))
