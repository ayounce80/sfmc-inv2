"""Tests for Asset extractor, particularly CloudPage AMPscript parsing."""

import pytest

from sfmc_inv2.extractors.asset import AssetExtractor, CLOUDPAGE_ASSET_TYPES
from sfmc_inv2.extractors.base_extractor import ExtractorResult
from sfmc_inv2.types.relationships import RelationshipType


class TestAMPscriptBlockExtraction:
    """Tests for AMPscript block extraction."""

    @pytest.fixture
    def extractor(self):
        """Create an AssetExtractor instance for testing."""
        extractor = AssetExtractor.__new__(AssetExtractor)
        return extractor

    def test_extract_block_syntax(self, extractor):
        """Test extracting %%[ ... ]%% blocks."""
        content = '''
        <html>
        %%[SET @var = "test"]%%
        <body>Hello</body>
        </html>
        '''
        blocks = extractor._extract_ampscript_blocks(content)
        assert 'SET @var = "test"' in blocks

    def test_extract_inline_syntax(self, extractor):
        """Test extracting %%= ... =%% inline expressions."""
        content = '<p>Hello %%=v(@name)=%%!</p>'
        blocks = extractor._extract_ampscript_blocks(content)
        assert "v(@name)" in blocks

    def test_extract_multiline_block(self, extractor):
        """Test extracting multiline AMPscript blocks."""
        content = '''
        %%[
        SET @var1 = "test"
        SET @var2 = "test2"
        ]%%
        '''
        blocks = extractor._extract_ampscript_blocks(content)
        assert "@var1" in blocks
        assert "@var2" in blocks

    def test_ignores_non_ampscript(self, extractor):
        """Test that non-AMPscript content is ignored."""
        content = '''
        <script>
        function Lookup(table, field) { return "fake"; }
        InsertDE("fake_table", data);
        </script>
        '''
        blocks = extractor._extract_ampscript_blocks(content)
        assert blocks == ""

    def test_empty_content(self, extractor):
        """Test empty content returns empty string."""
        assert extractor._extract_ampscript_blocks("") == ""
        assert extractor._extract_ampscript_blocks(None) == ""


class TestAMPscriptParser:
    """Tests for AMPscript DE reference parsing."""

    @pytest.fixture
    def extractor(self):
        """Create an AssetExtractor instance for testing."""
        extractor = AssetExtractor.__new__(AssetExtractor)
        return extractor

    def test_parse_insert_de(self, extractor):
        """Test parsing InsertDE function."""
        content = '''
        %%[
        InsertDE("dtc_web_signups", "email", @email, "signup_date", Now())
        ]%%
        '''
        refs = extractor._parse_ampscript_de_refs(content)
        assert len(refs) == 1
        assert refs[0] == ("dtc_web_signups", "insert")

    def test_parse_upsert_de(self, extractor):
        """Test parsing UpsertDE function."""
        content = '''%%[UpsertDE("Customer_Preferences", "CustomerID", @custId)]%%'''
        refs = extractor._parse_ampscript_de_refs(content)
        assert len(refs) == 1
        assert refs[0] == ("Customer_Preferences", "upsert")

    def test_parse_update_data(self, extractor):
        """Test parsing UpdateData function."""
        content = '''%%[UpdateData("Lead_Status", 1, "LeadID", @leadId, "Status", "Active")]%%'''
        refs = extractor._parse_ampscript_de_refs(content)
        assert len(refs) == 1
        assert refs[0] == ("Lead_Status", "update")

    def test_parse_delete_de(self, extractor):
        """Test parsing DeleteDE function."""
        content = '''%%[DeleteDE("Temp_Data", "ID", @recordId)]%%'''
        refs = extractor._parse_ampscript_de_refs(content)
        assert len(refs) == 1
        assert refs[0] == ("Temp_Data", "delete")

    def test_parse_lookup(self, extractor):
        """Test parsing Lookup function."""
        content = '''%%[SET @name = Lookup("Subscribers", "FirstName", "Email", @email)]%%'''
        refs = extractor._parse_ampscript_de_refs(content)
        assert len(refs) == 1
        assert refs[0] == ("Subscribers", "read")

    def test_parse_lookup_rows(self, extractor):
        """Test parsing LookupRows function."""
        content = '''%%[SET @rows = LookupRows("Order_History", "CustomerID", @custId)]%%'''
        refs = extractor._parse_ampscript_de_refs(content)
        assert len(refs) == 1
        assert refs[0] == ("Order_History", "read")

    def test_parse_lookup_ordered_rows(self, extractor):
        """Test parsing LookupOrderedRows function."""
        content = '''%%[
        SET @rows = LookupOrderedRows("Products", 10, "Price DESC", "Category", @cat)
        ]%%'''
        refs = extractor._parse_ampscript_de_refs(content)
        assert len(refs) == 1
        assert refs[0] == ("Products", "read")

    def test_parse_claim_row(self, extractor):
        """Test parsing ClaimRow function."""
        content = '''%%[SET @row = ClaimRow("Coupon_Codes", "IsClaimed", "EmailAddress", @email)]%%'''
        refs = extractor._parse_ampscript_de_refs(content)
        assert len(refs) == 1
        assert refs[0] == ("Coupon_Codes", "read")

    def test_parse_multiple_refs(self, extractor):
        """Test parsing multiple DE references in one content block."""
        content = '''
        %%[
        SET @profile = Lookup("Customer_Profile", "Name", "Email", @email)
        SET @history = LookupRows("Order_History", "Email", @email)
        InsertDE("Web_Activity", "Email", @email, "PageView", @page)
        UpsertDE("Session_Tracking", "SessionID", @sessionId, "LastSeen", Now())
        ]%%
        '''
        refs = extractor._parse_ampscript_de_refs(content)
        assert len(refs) == 4

        # Check reads
        reads = [r for r in refs if r[1] == "read"]
        assert len(reads) == 2
        read_names = {r[0] for r in reads}
        assert "Customer_Profile" in read_names
        assert "Order_History" in read_names

        # Check writes
        writes = [r for r in refs if r[1] in ("insert", "upsert")]
        assert len(writes) == 2
        write_names = {r[0] for r in writes}
        assert "Web_Activity" in write_names
        assert "Session_Tracking" in write_names

    def test_parse_case_insensitive(self, extractor):
        """Test that parsing is case-insensitive."""
        content = '''
        %%[
        insertde("Table1", "Col", @val)
        INSERTDE("Table2", "Col", @val)
        LOOKUP("Table3", "Col", "Key", @key)
        lookup("Table4", "Col", "Key", @key)
        ]%%
        '''
        refs = extractor._parse_ampscript_de_refs(content)
        assert len(refs) == 4

    def test_parse_single_quotes(self, extractor):
        """Test parsing with single quotes."""
        content = "%%[InsertDE('My_Data_Extension', 'Col', @val)]%%"
        refs = extractor._parse_ampscript_de_refs(content)
        assert len(refs) == 1
        assert refs[0][0] == "My_Data_Extension"

    def test_parse_empty_content(self, extractor):
        """Test parsing empty content returns empty list."""
        assert extractor._parse_ampscript_de_refs("") == []
        assert extractor._parse_ampscript_de_refs(None) == []

    def test_parse_no_ampscript_blocks(self, extractor):
        """Test parsing content without AMPscript blocks returns empty."""
        content = "<html><body>Hello World</body></html>"
        refs = extractor._parse_ampscript_de_refs(content)
        assert refs == []

    def test_ignores_javascript_false_positives(self, extractor):
        """Test that JavaScript containing similar function names is ignored."""
        content = '''
        <html>
        <script>
        // This should NOT be parsed
        function Lookup(table, field) { return data[table][field]; }
        InsertDE("fake_table", {"col": "value"});
        </script>
        %%[
        // This SHOULD be parsed
        SET @real = Lookup("Real_Table", "Field", "Key", @key)
        ]%%
        </html>
        '''
        refs = extractor._parse_ampscript_de_refs(content)
        # Should only find the real AMPscript reference
        assert len(refs) == 1
        assert refs[0] == ("Real_Table", "read")


class TestCloudPageAssetTypes:
    """Tests for CloudPage asset type constants."""

    def test_cloudpage_types_defined(self):
        """Test that CloudPage asset types are defined."""
        assert 205 in CLOUDPAGE_ASSET_TYPES  # Webpage
        assert 212 in CLOUDPAGE_ASSET_TYPES  # Landing Page
        assert 247 in CLOUDPAGE_ASSET_TYPES  # CloudPages


class TestExtractContentText:
    """Tests for content extraction helper."""

    @pytest.fixture
    def extractor(self):
        """Create an AssetExtractor instance for testing."""
        extractor = AssetExtractor.__new__(AssetExtractor)
        return extractor

    def test_extract_direct_content(self, extractor):
        """Test extracting direct content field."""
        item = {"content": "Hello World"}
        result = extractor._extract_content_text(item)
        assert "Hello World" in result

    def test_extract_views_content(self, extractor):
        """Test extracting content from views."""
        item = {
            "views": {
                "html": {"content": "<html>Test</html>"},
                "text": {"content": "Plain text"},
            }
        }
        result = extractor._extract_content_text(item)
        assert "<html>Test</html>" in result
        assert "Plain text" in result

    def test_extract_combined_content(self, extractor):
        """Test extracting combined content from multiple sources."""
        item = {
            "content": "Direct content",
            "views": {
                "html": {"content": "View content"},
            }
        }
        result = extractor._extract_content_text(item)
        assert "Direct content" in result
        assert "View content" in result

    def test_extract_empty_item(self, extractor):
        """Test extracting from empty item."""
        result = extractor._extract_content_text({})
        assert result == ""


class TestRelationshipTypes:
    """Tests for CloudPage relationship types."""

    def test_cloudpage_writes_de_exists(self):
        """Test CLOUDPAGE_WRITES_DE relationship type exists."""
        assert hasattr(RelationshipType, "CLOUDPAGE_WRITES_DE")
        assert RelationshipType.CLOUDPAGE_WRITES_DE.value == "cloudpage_writes_de"

    def test_cloudpage_reads_de_exists(self):
        """Test CLOUDPAGE_READS_DE relationship type exists."""
        assert hasattr(RelationshipType, "CLOUDPAGE_READS_DE")
        assert RelationshipType.CLOUDPAGE_READS_DE.value == "cloudpage_reads_de"


class TestExtractRelationshipsIntegration:
    """Integration tests for extract_relationships() with CloudPages."""

    @pytest.fixture
    def extractor(self):
        """Create an AssetExtractor instance for testing."""
        extractor = AssetExtractor.__new__(AssetExtractor)
        return extractor

    @pytest.mark.asyncio
    async def test_cloudpage_writes_de_relationship(self, extractor):
        """Test that CloudPage write operations create correct relationships."""
        items = [{
            "id": "12345",
            "name": "Signup Form",
            "assetType": {"id": 205},  # Webpage - CloudPage type
            "content": '''
            %%[
            InsertDE("dtc_web_signups", "email", @email, "timestamp", Now())
            ]%%
            ''',
        }]

        result = ExtractorResult(extractor_name="assets")
        await extractor.extract_relationships(items, result)

        # Should have one relationship
        assert len(result.relationships) == 1
        edge = result.relationships[0]

        # Verify relationship properties
        assert edge.source_id == "12345"
        assert edge.source_type == "asset"
        assert edge.source_name == "Signup Form"
        assert edge.target_id == "dtc_web_signups"
        assert edge.target_type == "data_extension"
        assert edge.relationship_type == RelationshipType.CLOUDPAGE_WRITES_DE

        # Verify metadata
        assert edge.metadata is not None
        assert edge.metadata.get("resolved_by_name") is True
        assert edge.metadata.get("operation") == "insert"
        assert edge.metadata.get("asset_type") == "cloudpage"

    @pytest.mark.asyncio
    async def test_cloudpage_reads_de_relationship(self, extractor):
        """Test that CloudPage read operations create correct relationships."""
        items = [{
            "id": "67890",
            "name": "Profile Page",
            "assetType": {"id": 247},  # CloudPages type
            "content": '''
            %%[
            SET @profile = Lookup("Customer_Profile", "Name", "Email", @email)
            ]%%
            ''',
        }]

        result = ExtractorResult(extractor_name="assets")
        await extractor.extract_relationships(items, result)

        assert len(result.relationships) == 1
        edge = result.relationships[0]

        assert edge.target_id == "Customer_Profile"
        assert edge.relationship_type == RelationshipType.CLOUDPAGE_READS_DE
        assert edge.metadata.get("resolved_by_name") is True
        assert edge.metadata.get("operation") == "read"

    @pytest.mark.asyncio
    async def test_cloudpage_multiple_de_refs(self, extractor):
        """Test CloudPage with multiple DE references creates multiple edges."""
        items = [{
            "id": "11111",
            "name": "Complex Page",
            "assetType": {"id": 212},  # Landing Page
            "content": '''
            %%[
            SET @user = Lookup("Users", "Name", "ID", @userId)
            UpsertDE("Page_Views", "UserID", @userId, "PageName", "complex")
            ]%%
            ''',
        }]

        result = ExtractorResult(extractor_name="assets")
        await extractor.extract_relationships(items, result)

        assert len(result.relationships) == 2

        # Find read and write edges
        read_edges = [e for e in result.relationships
                      if e.relationship_type == RelationshipType.CLOUDPAGE_READS_DE]
        write_edges = [e for e in result.relationships
                       if e.relationship_type == RelationshipType.CLOUDPAGE_WRITES_DE]

        assert len(read_edges) == 1
        assert len(write_edges) == 1
        assert read_edges[0].target_id == "Users"
        assert write_edges[0].target_id == "Page_Views"

    @pytest.mark.asyncio
    async def test_non_cloudpage_skips_ampscript_parsing(self, extractor):
        """Test that non-CloudPage assets don't get AMPscript parsing."""
        items = [{
            "id": "22222",
            "name": "Regular Email",
            "assetType": {"id": 207},  # Template-Based Email - NOT a CloudPage
            "content": '''
            %%[
            InsertDE("Some_Table", "col", @val)
            ]%%
            ''',
        }]

        result = ExtractorResult(extractor_name="assets")
        await extractor.extract_relationships(items, result)

        # Should NOT create CloudPage relationships for non-CloudPage types
        cloudpage_edges = [e for e in result.relationships
                          if e.relationship_type in (
                              RelationshipType.CLOUDPAGE_WRITES_DE,
                              RelationshipType.CLOUDPAGE_READS_DE
                          )]
        assert len(cloudpage_edges) == 0

    @pytest.mark.asyncio
    async def test_deduplicates_same_de_refs(self, extractor):
        """Test that multiple refs to same DE only create one edge per type."""
        items = [{
            "id": "33333",
            "name": "Multi-Lookup Page",
            "assetType": {"id": 205},
            "content": '''
            %%[
            SET @v1 = Lookup("Same_Table", "Col1", "Key", @k1)
            SET @v2 = Lookup("Same_Table", "Col2", "Key", @k2)
            InsertDE("Same_Table", "Col3", @v3)
            ]%%
            ''',
        }]

        result = ExtractorResult(extractor_name="assets")
        await extractor.extract_relationships(items, result)

        # Should deduplicate: 1 read edge + 1 write edge (not 2 reads + 1 write)
        assert len(result.relationships) == 2
        read_edges = [e for e in result.relationships
                      if e.relationship_type == RelationshipType.CLOUDPAGE_READS_DE]
        write_edges = [e for e in result.relationships
                       if e.relationship_type == RelationshipType.CLOUDPAGE_WRITES_DE]
        assert len(read_edges) == 1
        assert len(write_edges) == 1
