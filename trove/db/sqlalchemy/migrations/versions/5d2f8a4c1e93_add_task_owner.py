# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""add task_owner to instances and clusters

The task manager process carrying out the task of an instance or a
cluster while it does; NULL is none.

Revision ID: 5d2f8a4c1e93
Revises: 3c8b2e6f9a41
Create Date: 2026-10-19 15:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5d2f8a4c1e93'
down_revision: Union[str, None] = '3c8b2e6f9a41'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    for table in ('instances', 'clusters'):
        op.add_column(table, sa.Column('task_owner', sa.String(255),
                                       nullable=True))


def downgrade() -> None:
    for table in ('instances', 'clusters'):
        op.drop_column(table, 'task_owner')
