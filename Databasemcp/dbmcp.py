

import os
import logging
from dbmcp.server import MCPServer

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("dbmcp.log"),
        logging.StreamHandler()
    ]
)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="MCP 서버 실행")
    parser.add_argument('--readonly', action='store_true', help='readonly 모드로 실행 (DB 쓰기 차단)')
    args = parser.parse_args()

    try:
        mcp = MCPServer(config_path=os.path.join(os.path.dirname(__file__), 'servers.json'), readonly=args.readonly)
        print("서버 목록:", list(mcp.servers.keys()))
        # 예시 쿼리 실행 (사용자에 맞게 수정)
        # result = mcp.execute_on('my_postgres', 'SELECT version();')
        # print(result)
    except Exception as e:
        logging.critical(f"MCP 서버 실행 중 치명적 오류: {e}")
    finally:
        if 'mcp' in locals():
            mcp.close_all()


