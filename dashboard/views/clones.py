import streamlit as st

from dashboard import ui

st.title("Clones")
st.markdown(
    "Title-clone families: games whose titles reduce to the same words once emojis, bracket "
    "tags like `[UPD]`, numbers and filler words are removed. Small variants (\"Steal a "
    "Brainrot **Duels**\") are linked to their family. Families can span both platforms.")

c1, c2 = st.columns([1, 2])
cross = c1.toggle("Only families on both platforms", value=False, key="c_cross")
min_size = c2.slider("Minimum titles in the family", 2, 50, 3, key="c_min")
fams = ui.get("clone_families", min_size, cross)
st.caption(f"{len(fams):,} families")
event = st.dataframe(
    fams, hide_index=True, width="stretch", on_select="rerun",
    selection_mode="single-row", key="c_table", height=320,
    column_config={"family": "Family (key words)", "titles": "Titles",
                   "roblox": st.column_config.NumberColumn("Roblox", format="%d"),
                   "fortnite": st.column_config.NumberColumn("Fortnite", format="%d")})
if fams.empty:
    st.stop()
family = fams.iloc[event.selection.rows[0] if event.selection.rows else 0]["family"]

st.subheader(f"“{family}”")
members = ui.get("clone_members", family)
st.dataframe(
    members, hide_index=True, width="stretch",
    column_order=["platform", "name", "launched_at", "clone_rank", "peak_ccu"],
    column_config={
        "platform": "Platform", "name": "Name",
        "launched_at": st.column_config.DatetimeColumn("Launched", format="YYYY-MM-DD"),
        "clone_rank": st.column_config.NumberColumn("Order in family", format="%d"),
        "peak_ccu": st.column_config.NumberColumn("Peak CCU (observed)", format="localized")})
st.caption("Roblox dates are creation dates. Fortnite does not publish them, so Fortnite dates "
           "are when the tracker first saw the island (the catalog was first loaded on "
           "2026-11-10).")
