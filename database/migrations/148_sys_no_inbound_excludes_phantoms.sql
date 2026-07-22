-- 148_sys_no_inbound_excludes_phantoms.sql
-- sys.v_no_inbound is the R1 cleanup candidate list. The first scan returned 657 workbooks
-- with "no code references", but 132 of those are not workbooks at all -- they are phantom
-- nodes minted for external-link targets that do not exist on disk (Dropbox paths, a CONAB
-- network share, Eurostat temp folders). A phantom is by construction the SOURCE of its
-- LINKS_TO edge and never the target, so it has no inbound edge and lands on the list.
--
-- That inflates the cleanup denominator with files that cannot be cleaned up because they are
-- not here. An archive-candidate report is only useful if every row on it is a real artifact,
-- so unresolved nodes are excluded. They are already reported separately, as broken links.

CREATE OR REPLACE VIEW sys.v_no_inbound AS
SELECT n.node_type, n.node_key, n.lifecycle, n.properties,
       CASE WHEN n.node_type IN ('workbook','worksheet')
            THEN 'no-code-references'      -- R4: a human opens these by double-clicking
            ELSE 'no-inbound-edges'
       END AS claim
FROM sys.v_node n
WHERE NOT EXISTS (SELECT 1 FROM sys.v_edge e WHERE e.target_node_id = n.node_id)
  AND n.node_type IN ('repo_file','sql_script','vba_module','workbook')
  AND n.resolution_status = 'resolved'
  AND COALESCE(n.properties->>'phantom', 'false') <> 'true';

COMMENT ON VIEW sys.v_no_inbound IS
    'R1 candidate list. For code, zero inbound edges is strong evidence of death. For a '
    'workbook it is nearly none -- the claim is only ever "no CODE references this". '
    'Candidates, never conclusions.';
