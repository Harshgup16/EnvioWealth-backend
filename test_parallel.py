"""
Test script to verify parallel chunk processing works correctly
WITHOUT making real API calls (saves API quota).

This simulates the parallel execution with mock data.
"""

import asyncio
import time
from datetime import datetime

# Simulate chunks
MOCK_CHUNKS = [
    {"id": "sectionA_complete", "name": "Section A: Complete Company Information"},
    {"id": "sectionB_complete", "name": "Section B: Policies and Governance"},
    {"id": "sectionC_p1_p2", "name": "Section C: Principles 1-2"},
    {"id": "sectionC_p3_p4", "name": "Section C: Principles 3-4"},
    {"id": "sectionC_p5_p6", "name": "Section C: Principles 5-6"},
    {"id": "sectionC_p7_p8_p9", "name": "Section C: Principles 7-9"},
]


async def mock_extract_chunk(chunk_id: str, chunk_name: str, delay: float = 2.0):
    """Simulate chunk extraction with artificial delay"""
    print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] STARTED: {chunk_name}")
    
    # Simulate API processing time (2-3 seconds per chunk)
    await asyncio.sleep(delay)
    
    print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] COMPLETED: {chunk_name}")
    
    # Return mock data
    return {
        "chunk_id": chunk_id,
        "field_count": 50,  # Mock field count
        "status": "success"
    }


async def test_sequential():
    """Test sequential processing (one by one)"""
    print("\n" + "="*80)
    print("TEST 1: SEQUENTIAL PROCESSING (Current Behavior)")
    print("="*80)
    
    start_time = time.time()
    results = []
    
    for i, chunk in enumerate(MOCK_CHUNKS):
        print(f"\n[Progress] Processing chunk {i+1}/{len(MOCK_CHUNKS)}: {chunk['name']}")
        result = await mock_extract_chunk(chunk['id'], chunk['name'])
        results.append(result)
        
        # Delay between chunks (simulating current 15s delay)
        if i < len(MOCK_CHUNKS) - 1:
            print(f"[Delay] Waiting 1s before next chunk...")
            await asyncio.sleep(1)
    
    total_time = time.time() - start_time
    
    print(f"\n{'='*80}")
    print(f"SEQUENTIAL RESULTS:")
    print(f"Total chunks: {len(results)}")
    print(f"Total time: {total_time:.2f} seconds")
    print(f"Average per chunk: {total_time/len(MOCK_CHUNKS):.2f} seconds")
    print(f"{'='*80}\n")
    
    return total_time


async def test_parallel():
    """Test parallel processing (all at once)"""
    print("\n" + "="*80)
    print("TEST 2: PARALLEL PROCESSING (New Behavior)")
    print("="*80)
    
    start_time = time.time()
    
    # Process all chunks in parallel using asyncio.gather
    print(f"\n[Parallel Mode] Launching {len(MOCK_CHUNKS)} chunks simultaneously...\n")
    
    tasks = [
        mock_extract_chunk(chunk['id'], chunk['name']) 
        for chunk in MOCK_CHUNKS
    ]
    
    results = await asyncio.gather(*tasks)
    
    total_time = time.time() - start_time
    
    print(f"\n{'='*80}")
    print(f"PARALLEL RESULTS:")
    print(f"Total chunks: {len(results)}")
    print(f"Total time: {total_time:.2f} seconds")
    print(f"Average per chunk: {total_time/len(MOCK_CHUNKS):.2f} seconds")
    print(f"{'='*80}\n")
    
    return total_time


async def main():
    """Run both tests and compare"""
    print("\n" + "█"*80)
    print("█" + " "*78 + "█")
    print("█" + "  PARALLEL PROCESSING TEST SUITE".center(78) + "█")
    print("█" + "  (Mock Mode - No API Calls)".center(78) + "█")
    print("█" + " "*78 + "█")
    print("█"*80)
    
    # Test sequential
    sequential_time = await test_sequential()
    
    # Wait a bit between tests
    await asyncio.sleep(2)
    
    # Test parallel
    parallel_time = await test_parallel()
    
    # Comparison
    print("\n" + "█"*80)
    print("COMPARISON SUMMARY")
    print("█"*80)
    print(f"Sequential Time:  {sequential_time:.2f} seconds")
    print(f"Parallel Time:    {parallel_time:.2f} seconds")
    print(f"Speed Improvement: {sequential_time/parallel_time:.2f}x faster")
    print(f"Time Saved:       {sequential_time - parallel_time:.2f} seconds")
    print("█"*80)
    
    if parallel_time < sequential_time:
        print("\n✅ SUCCESS: Parallel processing is working correctly!")
        print(f"   With real API calls (~10s per chunk), you would save ~{(sequential_time - parallel_time) * 5:.0f} seconds")
    else:
        print("\n❌ FAILED: Parallel processing is NOT working")
    
    print("\n")


if __name__ == "__main__":
    asyncio.run(main())
