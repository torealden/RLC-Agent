#!/usr/bin/env python3
"""
Document RAG System for RLC Master Agent

Uses Ollama's nomic-embed-text model to create embeddings and enable
semantic search across Excel files, PDFs, and Markdown documents.

Usage:
    # Index all documents
    python document_rag.py --index

    # Search for documents
    python document_rag.py --search "soybean balance sheet"

    # Rebuild index
    python document_rag.py --rebuild
"""

import os
import sys
import json
import hashlib
import argparse
import asyncio
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, asdict
import re

# Try to import optional dependencies
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

try:
    from pypdf import PdfReader
    PDF_AVAILABLE = True
except ImportError:
    try:
        from PyPDF2 import PdfReader
        PDF_AVAILABLE = True
    except ImportError:
        PDF_AVAILABLE = False


# Configuration
DEFAULT_OLLAMA_URL = "http://localhost:11434"
EMBED_MODEL = "nomic-embed-text"
CHUNK_SIZE = 500  # characters per chunk
CHUNK_OVERLAP = 50  # overlap between chunks
TOP_K_RESULTS = 5  # number of results to return


@dataclass
class DocumentChunk:
    """Represents a chunk of a document with its embedding."""
    doc_id: str
    file_path: str
    file_name: str
    file_type: str
    chunk_index: int
    content: str
    metadata: Dict[str, Any]
    embedding: Optional[List[float]] = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> 'DocumentChunk':
        return cls(**data)


@dataclass
class SearchResult:
    """A search result with similarity score."""
    chunk: DocumentChunk
    score: float

    def to_dict(self) -> dict:
        return {
            "file_path": self.chunk.file_path,
            "file_name": self.chunk.file_name,
            "file_type": self.chunk.file_type,
            "content": self.chunk.content,
            "metadata": self.chunk.metadata,
            "score": self.score
        }


class OllamaEmbedder:
    """Generate embeddings using Ollama's nomic-embed-text model."""

    def __init__(self, base_url: str = DEFAULT_OLLAMA_URL, model: str = EMBED_MODEL):
        self.base_url = base_url
        self.model = model
        self._session = None

    async def _ensure_session(self):
        if self._session is None:
            import aiohttp
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        if self._session:
            await self._session.close()
            self._session = None

    async def is_available(self) -> bool:
        """Check if Ollama is available and has the embedding model."""
        try:
            session = await self._ensure_session()
            async with session.get(f"{self.base_url}/api/tags", timeout=5) as resp:
                if resp.status != 200:
                    return False
                data = await resp.json()
                models = [m["name"] for m in data.get("models", [])]
                return any(EMBED_MODEL in m for m in models)
        except Exception as e:
            print(f"Ollama check failed: {e}")
            return False

    async def embed(self, text: str) -> List[float]:
        """Generate embedding for a single text."""
        session = await self._ensure_session()

        payload = {
            "model": self.model,
            "prompt": text
        }

        try:
            async with session.post(
                f"{self.base_url}/api/embeddings",
                json=payload,
                timeout=30
            ) as resp:
                if resp.status != 200:
                    error = await resp.text()
                    raise Exception(f"Embedding error: {error}")
                data = await resp.json()
                return data["embedding"]
        except Exception as e:
            print(f"Embedding failed: {e}")
            raise

    async def embed_batch(self, texts: List[str], batch_size: int = 10) -> List[List[float]]:
        """Generate embeddings for multiple texts."""
        embeddings = []
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            batch_embeddings = await asyncio.gather(
                *[self.embed(text) for text in batch]
            )
            embeddings.extend(batch_embeddings)
            if i + batch_size < len(texts):
                print(f"  Embedded {i + batch_size}/{len(texts)} chunks...")
        return embeddings


class DocumentProcessor:
    """Process different document types into text chunks."""

    @staticmethod
    def chunk_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> List[str]:
        """Split text into overlapping chunks."""
        if not text or len(text.strip()) == 0:
            return []

        # Clean up text
        text = re.sub(r'\s+', ' ', text).strip()

        if len(text) <= chunk_size:
            return [text]

        chunks = []
        start = 0
        while start < len(text):
            end = start + chunk_size

            # Try to break at sentence or word boundary
            if end < len(text):
                # Look for sentence end
                for sep in ['. ', '! ', '? ', '\n', ', ', ' ']:
                    last_sep = text[start:end].rfind(sep)
                    if last_sep > chunk_size // 2:
                        end = start + last_sep + len(sep)
                        break

            chunk = text[start:end].strip()
            if chunk:
                chunks.append(chunk)

            start = end - overlap

        return chunks

    @staticmethod
    def process_markdown(file_path: Path) -> Tuple[str, Dict[str, Any]]:
        """Extract text and metadata from Markdown file."""
        try:
            content = file_path.read_text(encoding='utf-8', errors='ignore')

            # Extract title from first heading
            title_match = re.search(r'^#\s+(.+)$', content, re.MULTILINE)
            title = title_match.group(1) if title_match else file_path.stem

            # Count headings for structure info
            headings = re.findall(r'^#+\s+.+$', content, re.MULTILINE)

            metadata = {
                "title": title,
                "heading_count": len(headings),
                "word_count": len(content.split())
            }

            return content, metadata
        except Exception as e:
            print(f"Error reading markdown {file_path}: {e}")
            return "", {}

    @staticmethod
    def process_excel(file_path: Path) -> Tuple[str, Dict[str, Any]]:
        """Extract text and metadata from Excel file."""
        if not PANDAS_AVAILABLE:
            return f"[Excel file: {file_path.name}]", {"error": "pandas not available"}

        try:
            # Read all sheets
            excel_file = pd.ExcelFile(file_path)
            sheet_names = excel_file.sheet_names

            all_text = []
            all_text.append(f"File: {file_path.name}")
            all_text.append(f"Sheets: {', '.join(sheet_names)}")

            for sheet_name in sheet_names[:10]:  # Limit to first 10 sheets
                try:
                    df = pd.read_excel(excel_file, sheet_name=sheet_name, nrows=100)

                    if df.empty:
                        continue

                    all_text.append(f"\n--- Sheet: {sheet_name} ---")

                    # Add column names
                    cols = [str(c) for c in df.columns if not str(c).startswith('Unnamed')]
                    if cols:
                        all_text.append(f"Columns: {', '.join(cols)}")

                    # Sample data from first few rows
                    for idx, row in df.head(5).iterrows():
                        row_text = ' | '.join([
                            f"{col}: {val}"
                            for col, val in row.items()
                            if pd.notna(val) and not str(col).startswith('Unnamed')
                        ][:10])  # Limit columns per row
                        if row_text:
                            all_text.append(row_text)

                except Exception as e:
                    all_text.append(f"[Error reading sheet {sheet_name}: {e}]")

            metadata = {
                "sheet_count": len(sheet_names),
                "sheets": sheet_names[:10],
                "type": "excel"
            }

            return '\n'.join(all_text), metadata

        except Exception as e:
            print(f"Error reading Excel {file_path}: {e}")
            return f"[Excel file: {file_path.name} - Error: {e}]", {"error": str(e)}

    @staticmethod
    def process_pdf(file_path: Path) -> Tuple[str, Dict[str, Any]]:
        """Extract text and metadata from PDF file."""
        if not PDF_AVAILABLE:
            return f"[PDF file: {file_path.name}]", {"error": "pypdf not available"}

        try:
            reader = PdfReader(file_path)

            all_text = []
            all_text.append(f"PDF: {file_path.name}")
            all_text.append(f"Pages: {len(reader.pages)}")

            for i, page in enumerate(reader.pages[:20]):  # Limit to first 20 pages
                try:
                    text = page.extract_text()
                    if text:
                        all_text.append(f"\n--- Page {i+1} ---")
                        all_text.append(text)
                except Exception as e:
                    all_text.append(f"[Error on page {i+1}: {e}]")

            metadata = {
                "page_count": len(reader.pages),
                "type": "pdf"
            }

            return '\n'.join(all_text), metadata

        except Exception as e:
            print(f"Error reading PDF {file_path}: {e}")
            return f"[PDF file: {file_path.name} - Error: {e}]", {"error": str(e)}


class VectorStore:
    """Simple JSON-based vector store for document embeddings."""

    def __init__(self, store_path: Path):
        self.store_path = store_path
        self.chunks: List[DocumentChunk] = []
        self.file_hashes: Dict[str, str] = {}
        self._load()

    def _load(self):
        """Load existing index from disk."""
        if self.store_path.exists():
            try:
                with open(self.store_path, 'r') as f:
                    data = json.load(f)
                self.chunks = [DocumentChunk.from_dict(c) for c in data.get("chunks", [])]
                self.file_hashes = data.get("file_hashes", {})
                print(f"Loaded {len(self.chunks)} chunks from index")
            except Exception as e:
                print(f"Error loading index: {e}")
                self.chunks = []
                self.file_hashes = {}

    def save(self):
        """Save index to disk."""
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "chunks": [c.to_dict() for c in self.chunks],
            "file_hashes": self.file_hashes,
            "updated_at": datetime.now().isoformat()
        }
        with open(self.store_path, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Saved {len(self.chunks)} chunks to index")

    def add_chunks(self, chunks: List[DocumentChunk]):
        """Add new chunks to the store."""
        self.chunks.extend(chunks)

    def remove_file(self, file_path: str):
        """Remove all chunks from a specific file."""
        self.chunks = [c for c in self.chunks if c.file_path != file_path]
        if file_path in self.file_hashes:
            del self.file_hashes[file_path]

    def file_needs_update(self, file_path: str, current_hash: str) -> bool:
        """Check if a file needs to be re-indexed."""
        return self.file_hashes.get(file_path) != current_hash

    def set_file_hash(self, file_path: str, file_hash: str):
        """Store the hash for a file."""
        self.file_hashes[file_path] = file_hash

    @staticmethod
    def cosine_similarity(a: List[float], b: List[float]) -> float:
        """Calculate cosine similarity between two vectors."""
        import math

        dot_product = sum(x * y for x, y in zip(a, b))
        norm_a = math.sqrt(sum(x * x for x in a))
        norm_b = math.sqrt(sum(x * x for x in b))

        if norm_a == 0 or norm_b == 0:
            return 0.0

        return dot_product / (norm_a * norm_b)

    def search(self, query_embedding: List[float], top_k: int = TOP_K_RESULTS) -> List[SearchResult]:
        """Find most similar chunks to query embedding."""
        if not self.chunks:
            return []

        # Calculate similarity for each chunk
        scored = []
        for chunk in self.chunks:
            if chunk.embedding:
                score = self.cosine_similarity(query_embedding, chunk.embedding)
                scored.append(SearchResult(chunk=chunk, score=score))

        # Sort by score and return top K
        scored.sort(key=lambda x: x.score, reverse=True)
        return scored[:top_k]

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the index."""
        file_types = {}
        for chunk in self.chunks:
            ft = chunk.file_type
            file_types[ft] = file_types.get(ft, 0) + 1

        return {
            "total_chunks": len(self.chunks),
            "total_files": len(self.file_hashes),
            "chunks_by_type": file_types
        }


class DocumentRAG:
    """Main RAG system for document search and retrieval."""

    def __init__(
        self,
        project_root: Path,
        ollama_url: str = DEFAULT_OLLAMA_URL
    ):
        self.project_root = project_root
        self.data_dir = project_root / "data"
        self.index_path = self.data_dir / "document_index.json"

        self.embedder = OllamaEmbedder(ollama_url)
        self.store = VectorStore(self.index_path)
        self.processor = DocumentProcessor()

        # Directories to index
        self.index_dirs = [
            project_root / "Models",
            project_root / "docs",
            project_root / "commodity_pipeline",
        ]

        # File patterns to include
        self.patterns = ["*.xlsx", "*.md", "*.pdf"]

    @staticmethod
    def get_file_hash(file_path: Path) -> str:
        """Get hash of file modification time and size."""
        stat = file_path.stat()
        return hashlib.md5(
            f"{stat.st_mtime}:{stat.st_size}".encode()
        ).hexdigest()

    def find_documents(self) -> List[Path]:
        """Find all documents to index."""
        documents = []

        for index_dir in self.index_dirs:
            if not index_dir.exists():
                continue

            for pattern in self.patterns:
                documents.extend(index_dir.rglob(pattern))

        return documents

    async def index_file(self, file_path: Path) -> List[DocumentChunk]:
        """Index a single file and return its chunks."""
        file_type = file_path.suffix.lower()

        # Process based on file type
        if file_type == '.md':
            content, metadata = self.processor.process_markdown(file_path)
        elif file_type == '.xlsx':
            content, metadata = self.processor.process_excel(file_path)
        elif file_type == '.pdf':
            content, metadata = self.processor.process_pdf(file_path)
        else:
            return []

        if not content:
            return []

        # Create chunks
        text_chunks = self.processor.chunk_text(content)

        chunks = []
        for i, text in enumerate(text_chunks):
            chunk = DocumentChunk(
                doc_id=hashlib.md5(f"{file_path}:{i}".encode()).hexdigest(),
                file_path=str(file_path),
                file_name=file_path.name,
                file_type=file_type,
                chunk_index=i,
                content=text,
                metadata=metadata
            )
            chunks.append(chunk)

        return chunks

    async def build_index(self, force_rebuild: bool = False):
        """Build or update the document index."""
        print("\n" + "=" * 60)
        print("  Document RAG - Building Index")
        print("=" * 60)

        # Check Ollama
        if not await self.embedder.is_available():
            print(f"\n{EMBED_MODEL} not available in Ollama")
            print("Please run: ollama pull nomic-embed-text")
            return False

        print(f"\nUsing embedding model: {EMBED_MODEL}")

        # Find documents
        documents = self.find_documents()
        print(f"Found {len(documents)} documents to process")

        # Process each document
        new_chunks = []
        updated_files = 0

        for doc_path in documents:
            file_hash = self.get_file_hash(doc_path)
            str_path = str(doc_path)

            # Check if file needs updating
            if not force_rebuild and not self.store.file_needs_update(str_path, file_hash):
                continue

            print(f"\nIndexing: {doc_path.name}")

            # Remove old chunks for this file
            self.store.remove_file(str_path)

            # Process file
            chunks = await self.index_file(doc_path)

            if chunks:
                # Generate embeddings
                print(f"  Creating {len(chunks)} embeddings...")
                texts = [c.content for c in chunks]
                embeddings = await self.embedder.embed_batch(texts)

                for chunk, embedding in zip(chunks, embeddings):
                    chunk.embedding = embedding

                new_chunks.extend(chunks)
                self.store.set_file_hash(str_path, file_hash)
                updated_files += 1

        # Add new chunks to store
        if new_chunks:
            self.store.add_chunks(new_chunks)
            self.store.save()

        print(f"\n{'=' * 60}")
        print(f"Indexing Complete!")
        print(f"  Updated files: {updated_files}")
        print(f"  New chunks: {len(new_chunks)}")
        print(f"  Total chunks: {len(self.store.chunks)}")
        print(f"{'=' * 60}\n")

        return True

    async def search(self, query: str, top_k: int = TOP_K_RESULTS) -> List[SearchResult]:
        """Search for documents matching the query."""
        if not self.store.chunks:
            print("Index is empty. Run with --index first.")
            return []

        # Generate query embedding
        query_embedding = await self.embedder.embed(query)

        # Search
        results = self.store.search(query_embedding, top_k)

        return results

    async def close(self):
        """Clean up resources."""
        await self.embedder.close()


# Tool interface for agent_tools.py
def search_documents_sync(query: str, top_k: int = 5) -> Dict[str, Any]:
    """
    Synchronous wrapper for document search.
    Used by agent_tools.py.
    """
    async def _search():
        project_root = Path(__file__).parent.parent
        rag = DocumentRAG(project_root)

        try:
            if not rag.store.chunks:
                return {
                    "success": False,
                    "error": "Document index is empty. Run: python document_rag.py --index"
                }

            results = await rag.search(query, top_k)

            return {
                "success": True,
                "query": query,
                "result_count": len(results),
                "results": [r.to_dict() for r in results]
            }
        finally:
            await rag.close()

    return asyncio.run(_search())


def get_index_stats() -> Dict[str, Any]:
    """Get statistics about the document index."""
    project_root = Path(__file__).parent.parent
    index_path = project_root / "data" / "document_index.json"

    if not index_path.exists():
        return {
            "indexed": False,
            "message": "No index found. Run: python document_rag.py --index"
        }

    store = VectorStore(index_path)
    stats = store.get_stats()
    stats["indexed"] = True
    stats["index_path"] = str(index_path)

    return stats


async def main():
    """CLI interface for document RAG."""
    parser = argparse.ArgumentParser(description="Document RAG System")
    parser.add_argument("--index", action="store_true", help="Build/update the document index")
    parser.add_argument("--rebuild", action="store_true", help="Force rebuild entire index")
    parser.add_argument("--search", type=str, help="Search query")
    parser.add_argument("--stats", action="store_true", help="Show index statistics")
    parser.add_argument("--ollama-url", default=DEFAULT_OLLAMA_URL, help="Ollama API URL")

    args = parser.parse_args()

    project_root = Path(__file__).parent.parent
    rag = DocumentRAG(project_root, args.ollama_url)

    try:
        if args.stats:
            stats = get_index_stats()
            print("\n--- Document Index Statistics ---")
            for key, value in stats.items():
                print(f"  {key}: {value}")
            print()
            return

        if args.index or args.rebuild:
            await rag.build_index(force_rebuild=args.rebuild)

        if args.search:
            print(f"\nSearching for: '{args.search}'")
            results = await rag.search(args.search)

            if not results:
                print("No results found.")
            else:
                print(f"\nFound {len(results)} results:\n")
                for i, result in enumerate(results, 1):
                    print(f"--- Result {i} (score: {result.score:.3f}) ---")
                    print(f"File: {result.chunk.file_name}")
                    print(f"Path: {result.chunk.file_path}")
                    print(f"Content: {result.chunk.content[:300]}...")
                    print()

        if not any([args.index, args.rebuild, args.search, args.stats]):
            parser.print_help()

    finally:
        await rag.close()


if __name__ == "__main__":
    asyncio.run(main())
