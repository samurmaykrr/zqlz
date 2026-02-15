// Stub for Zed's feature_flags crate

use gpui::App;

pub trait FeatureFlag: Send + Sync {
    const NAME: &'static str;

    fn enabled_for_staff(&self) -> bool {
        true
    }
}

pub struct DiffReviewFeatureFlag;

impl FeatureFlag for DiffReviewFeatureFlag {
    const NAME: &'static str = "diff_review";
}

pub trait FeatureFlagAppExt {
    fn is_feature_flag_enabled<F: FeatureFlag>(&self) -> bool;
}

impl FeatureFlagAppExt for App {
    fn is_feature_flag_enabled<F: FeatureFlag>(&self) -> bool {
        false
    }
}
