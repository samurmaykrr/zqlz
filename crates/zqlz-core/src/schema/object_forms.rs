use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::ObjectsPanelObjectRef;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectFormAction {
    pub kind_id: String,
    pub mode: ObjectFormMode,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ObjectFormMode {
    Create,
    Edit,
    Drop,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectFormSpecRequest {
    pub kind_id: String,
    pub mode: ObjectFormMode,
    pub object_ref: Option<ObjectsPanelObjectRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectFormDdlRequest {
    pub kind_id: String,
    pub mode: ObjectFormMode,
    pub object_ref: Option<ObjectsPanelObjectRef>,
    pub values: BTreeMap<String, ObjectFormValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectFormSpec {
    pub kind_id: String,
    pub mode: ObjectFormMode,
    pub title: String,
    pub sections: Vec<ObjectFormSection>,
}

impl ObjectFormSpec {
    pub fn new(kind_id: impl Into<String>, mode: ObjectFormMode, title: impl Into<String>) -> Self {
        Self {
            kind_id: kind_id.into(),
            mode,
            title: title.into(),
            sections: Vec::new(),
        }
    }

    pub fn sections(mut self, sections: Vec<ObjectFormSection>) -> Self {
        self.sections = sections;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectFormSection {
    pub title: Option<String>,
    pub fields: Vec<ObjectFormField>,
}

impl ObjectFormSection {
    pub fn new(fields: Vec<ObjectFormField>) -> Self {
        Self {
            title: None,
            fields,
        }
    }

    pub fn titled(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectFormField {
    pub id: String,
    pub label: String,
    pub kind: ObjectFormFieldKind,
    pub required: bool,
    pub placeholder: Option<String>,
    pub help_text: Option<String>,
    pub default_value: ObjectFormValue,
    pub options: Vec<ObjectFormOption>,
    pub read_only: bool,
}

impl ObjectFormField {
    pub fn new(id: impl Into<String>, label: impl Into<String>, kind: ObjectFormFieldKind) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            kind,
            required: false,
            placeholder: None,
            help_text: None,
            default_value: ObjectFormValue::String(String::new()),
            options: Vec::new(),
            read_only: false,
        }
    }

    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    pub fn placeholder(mut self, placeholder: impl Into<String>) -> Self {
        self.placeholder = Some(placeholder.into());
        self
    }

    pub fn help_text(mut self, help_text: impl Into<String>) -> Self {
        self.help_text = Some(help_text.into());
        self
    }

    pub fn default_value(mut self, value: ObjectFormValue) -> Self {
        self.default_value = value;
        self
    }

    pub fn options(mut self, options: Vec<ObjectFormOption>) -> Self {
        self.options = options;
        self
    }

    pub fn read_only(mut self) -> Self {
        self.read_only = true;
        self
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ObjectFormFieldKind {
    Text,
    TextArea,
    Select,
    Checkbox,
    StringList,
    KeyValueList,
    SqlExpression,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectFormOption {
    pub value: String,
    pub label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ObjectFormValue {
    String(String),
    Bool(bool),
    StringList(Vec<String>),
    KeyValueList(Vec<(String, String)>),
}

impl ObjectFormValue {
    pub fn as_string(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value.as_str()),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_string_list(&self) -> Option<&[String]> {
        match self {
            Self::StringList(value) => Some(value.as_slice()),
            _ => None,
        }
    }
}
