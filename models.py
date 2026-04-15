from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Enum as SQLEnum, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime
import enum

Base = declarative_base()

class PermissionLevel(enum.Enum):
    CREATOR = "creator"
    EDITOR = "editor"
    VIEWER = "viewer"
    RUNNER = "runner"

class Workflow(Base):
    __tablename__ = "workflows"
    
    id = Column(String(100), primary_key=True)
    name = Column(String(200))
    creator_id = Column(Integer)
    creator_username = Column(String(50))
    last_run = Column(String(20))
    workflow_data = Column(String(50000))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    permissions = relationship("WorkflowPermission", back_populates="workflow", cascade="all, delete-orphan")
    files = relationship("WorkflowFile", back_populates="workflow", cascade="all, delete-orphan")

class WorkflowPermission(Base):
    __tablename__ = "workflow_permissions"
    
    id = Column(Integer, primary_key=True, index=True)
    workflow_id = Column(String(100), ForeignKey("workflows.id"))
    user_id = Column(Integer)
    username = Column(String(50))
    permission_level = Column(SQLEnum(PermissionLevel))
    
    workflow = relationship("Workflow", back_populates="permissions")

class WorkflowFile(Base):
    __tablename__ = "workflow_files"
    
    id = Column(Integer, primary_key=True, index=True)
    workflow_id = Column(String(100), ForeignKey("workflows.id"))
    node_id = Column(String(100))
    backend_node_id = Column(String(100))
    filename = Column(String(200))
    saved_by = Column(String(50))
    rows = Column(Integer)
    columns = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    workflow = relationship("Workflow", back_populates="files")

class WorkflowVersion(Base):
    __tablename__ = "workflow_versions"
    
    id = Column(Integer, primary_key=True, index=True)
    workflow_id = Column(String(100), ForeignKey("workflows.id"))
    version_number = Column(Integer)
    workflow_data = Column(String(50000))
    created_by = Column(String(50))
    created_at = Column(DateTime, default=datetime.utcnow)
    comment = Column(String(500))
    
    runs = relationship("WorkflowRun", back_populates="version", cascade="all, delete-orphan")

class WorkflowRun(Base):
    __tablename__ = "workflow_runs"
    
    id = Column(Integer, primary_key=True, index=True)
    workflow_id = Column(String(100), ForeignKey("workflows.id"))
    version_id = Column(Integer, ForeignKey("workflow_versions.id"), nullable=True)
    run_by = Column(String(50))
    run_at = Column(DateTime, default=datetime.utcnow)
    status = Column(String(20))  # SUCCESS, ERROR, RUNNING
    saved_nodes = Column(String(10000), default="[]")  # JSON: [{node_id, backend_node_id, label, filename}]
    
    version = relationship("WorkflowVersion", back_populates="runs")


class FeatureTicket(Base):
    """Missing-feature tickets raised by the AI when a user request cannot be fulfilled."""
    __tablename__ = "feature_tickets"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String(300), nullable=False)
    description = Column(Text, nullable=False)          # what the feature should do
    category = Column(String(100))                      # e.g. "Machine Learning", "Visualization"
    use_case = Column(Text)                             # the original user prompt that triggered it
    requested_by = Column(String(100), default="ai_generator")
    vote_count = Column(Integer, default=1)             # starts at 1 (the triggering request)
    status = Column(String(30), default="open")         # open | in_progress | done
    missing_capability = Column(Text, default="")       # exact operation/algorithm missing
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class WorkflowSchedule(Base):
    """Cron schedule for automatic workflow execution."""
    __tablename__ = "workflow_schedules"

    id = Column(Integer, primary_key=True, index=True)
    workflow_id = Column(String(100), ForeignKey("workflows.id"), unique=True)
    cron_expression = Column(String(100), nullable=False)  # e.g. "0 9 * * 1-5" (9am weekdays)
    enabled = Column(Integer, default=1)                    # 1=enabled, 0=disabled
    timezone = Column(String(50), default="UTC")
    created_by = Column(String(100))
    last_run_at = Column(DateTime, nullable=True)
    next_run_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ScheduleRunLog(Base):
    """Log of each scheduled/triggered workflow run."""
    __tablename__ = "schedule_run_logs"

    id = Column(Integer, primary_key=True, index=True)
    workflow_id = Column(String(100), ForeignKey("workflows.id"))
    trigger_type = Column(String(20))       # "cron", "webhook", "manual"
    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    status = Column(String(20), default="running")  # running, success, error
    error_message = Column(Text, nullable=True)
    node_results = Column(Text, default="[]")       # JSON: [{node_id, backend_id, status}]
    triggered_by = Column(String(100), default="scheduler")
