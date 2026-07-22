-- 147_sys_positional_key_scope.sql
-- Narrow the no-positional-identity constraint (D3) to the node types where a cell address
-- could plausibly BE the identity.
--
-- The original constraint rejected any node_key ending in letters-then-digits after a '#'.
-- That is indistinguishable from a legitimate worksheet name: the first scan hit
-- `data/raw/EU - Renewable Energy Generation Annual Data.xlsx#EU27`, where EU27 is a sheet,
-- not cell EU27. A rule that cannot tell a sheet named Q1 from cell Q1 is not enforcing D3,
-- it is guessing.
--
-- D3's actual claim is narrower: a BLOCK inside a sheet is anchored by its column-A title
-- string, never by the row it currently occupies. `SoyOilRepointToFlatFile.bas` puts it best
-- in its own header -- "read it; never count rows." So the constraint now applies to
-- sheet_block and flat_file_series, where a positional key would be a real defect, and the
-- pattern additionally requires the `$` or `!` that a real Excel reference carries.

ALTER TABLE sys.node DROP CONSTRAINT IF EXISTS sys_node_no_positional_key_ck;

ALTER TABLE sys.node ADD CONSTRAINT sys_node_no_positional_key_ck CHECK (
    node_type NOT IN ('sheet_block', 'flat_file_series')
    OR node_key !~ '(#|!|\$)\$?[A-Z]{1,3}\$?[0-9]{1,7}$'
);

COMMENT ON CONSTRAINT sys_node_no_positional_key_ck ON sys.node IS
    'D3: Excel positions rot -- the oilseed folder alone holds 2.5M formula cells. Blocks are '
    'identified by their column-A title, flat-file rows by their series key. Cell addresses '
    'may be stored in edge.evidence as observations; never in a node_key.';
