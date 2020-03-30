"""Initialize path for uni_flow_converter package used by tests"""

import sys
from pathlib import Path

package_path = Path(__file__).parent.parent.joinpath("src/uni").as_posix()
sys.path.append(package_path)
