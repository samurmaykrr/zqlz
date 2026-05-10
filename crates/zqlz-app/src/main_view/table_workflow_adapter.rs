use gpui::*;
use uuid::Uuid;
use zqlz_services::{
    DeleteTablesDecision, DeleteTablesDecisionRequest, DesignTablesDecision,
    DesignTablesDecisionRequest, DuplicateTablesDecision, DuplicateTablesDecisionRequest,
    EmptyTablesDecision, EmptyTablesDecisionRequest, OpenTablesDecision, OpenTablesDecisionRequest,
    decide_delete_tables, decide_design_tables, decide_duplicate_tables, decide_empty_tables,
};
use zqlz_ui::widgets::WindowExt;
use zqlz_ui::widgets::notification::Notification;

use crate::main_view::MainView;

impl MainView {
    pub(in crate::main_view) fn decide_open_tables_workflow(
        &self,
        connection_id: Uuid,
        table_names: Vec<String>,
        database_name: Option<String>,
        is_view: bool,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<OpenTablesDecision> {
        match zqlz_services::decide_open_tables_workflow(OpenTablesDecisionRequest {
            connection_id,
            table_names,
            database_name,
            is_view,
        }) {
            Ok(decision) => Some(decision),
            Err(error) => {
                tracing::warn!(%error, "Failed to decide open tables workflow");
                window.push_notification(Notification::warning(error.to_string()), cx);
                None
            }
        }
    }

    pub(in crate::main_view) fn decide_design_tables_workflow(
        &self,
        connection_id: Uuid,
        table_names: Vec<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<DesignTablesDecision> {
        match decide_design_tables(DesignTablesDecisionRequest {
            connection_id,
            table_names,
        }) {
            Ok(decision) => Some(decision),
            Err(error) => {
                tracing::warn!(%error, "Failed to decide design tables workflow");
                window.push_notification(Notification::warning(error.to_string()), cx);
                None
            }
        }
    }

    pub(in crate::main_view) fn decide_delete_tables_workflow(
        &self,
        connection_id: Uuid,
        table_names: Vec<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<DeleteTablesDecision> {
        match decide_delete_tables(DeleteTablesDecisionRequest {
            connection_id,
            table_names,
        }) {
            Ok(decision) => Some(decision),
            Err(error) => {
                tracing::warn!(%error, "Failed to decide delete tables workflow");
                window.push_notification(Notification::warning(error.to_string()), cx);
                None
            }
        }
    }

    pub(in crate::main_view) fn decide_duplicate_tables_workflow(
        &self,
        connection_id: Uuid,
        table_names: Vec<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<DuplicateTablesDecision> {
        match decide_duplicate_tables(DuplicateTablesDecisionRequest {
            connection_id,
            table_names,
        }) {
            Ok(decision) => Some(decision),
            Err(error) => {
                tracing::warn!(%error, "Failed to decide duplicate tables workflow");
                window.push_notification(Notification::warning(error.to_string()), cx);
                None
            }
        }
    }

    pub(in crate::main_view) fn decide_empty_tables_workflow(
        &self,
        connection_id: Uuid,
        table_names: Vec<String>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Option<EmptyTablesDecision> {
        match decide_empty_tables(EmptyTablesDecisionRequest {
            connection_id,
            table_names,
        }) {
            Ok(decision) => Some(decision),
            Err(error) => {
                tracing::warn!(%error, "Failed to decide empty tables workflow");
                window.push_notification(Notification::warning(error.to_string()), cx);
                None
            }
        }
    }
}
