use std::fmt::{self, Display, Formatter};

use gpui::{AbsoluteLength, Axis, Length, Pixels};
use serde::{Deserialize, Serialize};

/// A enum for defining the placement of the element.
///
/// See also: [`Side`] if you need to define the left, right side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Placement {
    #[serde(rename = "top")]
    Top,
    #[serde(rename = "bottom")]
    Bottom,
    #[serde(rename = "left")]
    Left,
    #[serde(rename = "right")]
    Right,
}

impl Display for Placement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Placement::Top => write!(f, "Top"),
            Placement::Bottom => write!(f, "Bottom"),
            Placement::Left => write!(f, "Left"),
            Placement::Right => write!(f, "Right"),
        }
    }
}

impl Placement {
    #[inline]
    pub fn is_horizontal(&self) -> bool {
        match self {
            Placement::Left | Placement::Right => true,
            _ => false,
        }
    }

    #[inline]
    pub fn is_vertical(&self) -> bool {
        match self {
            Placement::Top | Placement::Bottom => true,
            _ => false,
        }
    }

    #[inline]
    pub fn axis(&self) -> Axis {
        match self {
            Placement::Top | Placement::Bottom => Axis::Vertical,
            Placement::Left | Placement::Right => Axis::Horizontal,
        }
    }
}

/// A enum for defining the side of the element.
///
/// See also: [`Placement`] if you need to define the 4 edges.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    #[serde(rename = "left")]
    Left,
    #[serde(rename = "right")]
    Right,
}

/// The anchor position of an element.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum Anchor {
    #[default]
    #[serde(rename = "top-left")]
    TopLeft,
    #[serde(rename = "top-center")]
    TopCenter,
    #[serde(rename = "top-right")]
    TopRight,
    #[serde(rename = "bottom-left")]
    BottomLeft,
    #[serde(rename = "bottom-center")]
    BottomCenter,
    #[serde(rename = "bottom-right")]
    BottomRight,
}

impl Display for Anchor {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Anchor::TopLeft => write!(f, "TopLeft"),
            Anchor::TopCenter => write!(f, "TopCenter"),
            Anchor::TopRight => write!(f, "TopRight"),
            Anchor::BottomLeft => write!(f, "BottomLeft"),
            Anchor::BottomCenter => write!(f, "BottomCenter"),
            Anchor::BottomRight => write!(f, "BottomRight"),
        }
    }
}

impl Anchor {
    /// Returns true if the anchor is at the top.
    #[inline]
    pub fn is_top(&self) -> bool {
        matches!(self, Self::TopLeft | Self::TopCenter | Self::TopRight)
    }

    /// Returns true if the anchor is at the bottom.
    #[inline]
    pub fn is_bottom(&self) -> bool {
        matches!(
            self,
            Self::BottomLeft | Self::BottomCenter | Self::BottomRight
        )
    }

    /// Returns true if the anchor is at the left.
    #[inline]
    pub fn is_left(&self) -> bool {
        matches!(self, Self::TopLeft | Self::BottomLeft)
    }

    /// Returns true if the anchor is at the right.
    #[inline]
    pub fn is_right(&self) -> bool {
        matches!(self, Self::TopRight | Self::BottomRight)
    }

    /// Returns true if the anchor is at the center.
    #[inline]
    pub fn is_center(&self) -> bool {
        matches!(self, Self::TopCenter | Self::BottomCenter)
    }

    /// Swaps the vertical position of the anchor.
    pub fn swap_vertical(&self) -> Self {
        match self {
            Anchor::TopLeft => Anchor::BottomLeft,
            Anchor::TopCenter => Anchor::BottomCenter,
            Anchor::TopRight => Anchor::BottomRight,
            Anchor::BottomLeft => Anchor::TopLeft,
            Anchor::BottomCenter => Anchor::TopCenter,
            Anchor::BottomRight => Anchor::TopRight,
        }
    }

    /// Swaps the horizontal position of the anchor.
    pub fn swap_horizontal(&self) -> Self {
        match self {
            Anchor::TopLeft => Anchor::TopRight,
            Anchor::TopCenter => Anchor::TopCenter,
            Anchor::TopRight => Anchor::TopLeft,
            Anchor::BottomLeft => Anchor::BottomRight,
            Anchor::BottomCenter => Anchor::BottomCenter,
            Anchor::BottomRight => Anchor::BottomLeft,
        }
    }

    /// Returns the anchor on the other side along the given axis.
    pub(crate) fn other_side_corner_along(&self, axis: Axis) -> Anchor {
        match axis {
            Axis::Vertical => match self {
                Self::TopLeft => Self::BottomLeft,
                Self::TopCenter => Self::BottomCenter,
                Self::TopRight => Self::BottomRight,
                Self::BottomLeft => Self::TopLeft,
                Self::BottomCenter => Self::TopCenter,
                Self::BottomRight => Self::TopRight,
            },
            Axis::Horizontal => match self {
                Self::TopLeft => Self::TopRight,
                Self::TopCenter => Self::TopCenter,
                Self::TopRight => Self::TopLeft,
                Self::BottomLeft => Self::BottomRight,
                Self::BottomCenter => Self::BottomCenter,
                Self::BottomRight => Self::BottomLeft,
            },
        }
    }
}

impl From<gpui::Corner> for Anchor {
    fn from(corner: gpui::Corner) -> Self {
        match corner {
            gpui::Corner::TopLeft => Anchor::TopLeft,
            gpui::Corner::TopRight => Anchor::TopRight,
            gpui::Corner::BottomLeft => Anchor::BottomLeft,
            gpui::Corner::BottomRight => Anchor::BottomRight,
        }
    }
}

impl From<Anchor> for gpui::Corner {
    fn from(anchor: Anchor) -> Self {
        match anchor {
            Anchor::TopLeft => gpui::Corner::TopLeft,
            Anchor::TopRight => gpui::Corner::TopRight,
            Anchor::BottomLeft => gpui::Corner::BottomLeft,
            Anchor::BottomRight => gpui::Corner::BottomRight,
            Anchor::TopCenter => gpui::Corner::TopLeft,
            Anchor::BottomCenter => gpui::Corner::BottomLeft,
        }
    }
}

impl Side {
    /// Returns true if the side is left.
    #[inline]
    pub fn is_left(&self) -> bool {
        matches!(self, Self::Left)
    }

    /// Returns true if the side is right.
    #[inline]
    pub fn is_right(&self) -> bool {
        matches!(self, Self::Right)
    }
}

/// A trait to extend the [`Axis`] enum with utility methods.
pub trait AxisExt {
    fn is_horizontal(self) -> bool;
    fn is_vertical(self) -> bool;
}

impl AxisExt for Axis {
    #[inline]
    fn is_horizontal(self) -> bool {
        self == Axis::Horizontal
    }

    #[inline]
    fn is_vertical(self) -> bool {
        self == Axis::Vertical
    }
}

/// A trait for converting [`Pixels`] to `f32` and `f64`.
pub trait PixelsExt {
    fn as_f32(&self) -> f32;
    fn as_f64(self) -> f64;
}
impl PixelsExt for Pixels {
    fn as_f32(&self) -> f32 {
        f32::from(self)
    }

    fn as_f64(self) -> f64 {
        f64::from(self)
    }
}

/// A trait to extend the [`Length`] enum with utility methods.
pub trait LengthExt {
    /// Converts the [`Length`] to [`Pixels`] based on a given `base_size` and `rem_size`.
    ///
    /// If the [`Length`] is [`Length::Auto`], it returns `None`.
    fn to_pixels(&self, base_size: AbsoluteLength, rem_size: Pixels) -> Option<Pixels>;
}

impl LengthExt for Length {
    fn to_pixels(&self, base_size: AbsoluteLength, rem_size: Pixels) -> Option<Pixels> {
        match self {
            Length::Auto => None,
            Length::Definite(len) => Some(len.to_pixels(base_size, rem_size)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Anchor, Placement};
    #[test]
    fn test_placement() {
        assert!(Placement::Left.is_horizontal());
        assert!(Placement::Right.is_horizontal());
        assert!(!Placement::Top.is_horizontal());
        assert!(!Placement::Bottom.is_horizontal());

        assert!(Placement::Top.is_vertical());
        assert!(Placement::Bottom.is_vertical());
        assert!(!Placement::Left.is_vertical());
        assert!(!Placement::Right.is_vertical());

        assert_eq!(Placement::Top.axis(), gpui::Axis::Vertical);
        assert_eq!(Placement::Bottom.axis(), gpui::Axis::Vertical);
        assert_eq!(Placement::Left.axis(), gpui::Axis::Horizontal);
        assert_eq!(Placement::Right.axis(), gpui::Axis::Horizontal);

        assert_eq!(Placement::Top.to_string(), "Top");
        assert_eq!(Placement::Bottom.to_string(), "Bottom");
        assert_eq!(Placement::Left.to_string(), "Left");
        assert_eq!(Placement::Right.to_string(), "Right");

        assert_eq!(serde_json::to_string(&Placement::Top).unwrap(), r#""top""#);
        assert_eq!(
            serde_json::to_string(&Placement::Bottom).unwrap(),
            r#""bottom""#
        );
        assert_eq!(
            serde_json::to_string(&Placement::Left).unwrap(),
            r#""left""#
        );
        assert_eq!(
            serde_json::to_string(&Placement::Right).unwrap(),
            r#""right""#
        );

        assert_eq!(
            serde_json::from_str::<Placement>(r#""top""#).unwrap(),
            Placement::Top
        );
        assert_eq!(
            serde_json::from_str::<Placement>(r#""bottom""#).unwrap(),
            Placement::Bottom
        );
        assert_eq!(
            serde_json::from_str::<Placement>(r#""left""#).unwrap(),
            Placement::Left
        );
        assert_eq!(
            serde_json::from_str::<Placement>(r#""right""#).unwrap(),
            Placement::Right
        );
    }

    #[test]
    fn test_side() {
        use super::Side;
        let left = Side::Left;
        let right = Side::Right;

        assert!(left.is_left());
        assert!(!left.is_right());

        assert!(right.is_right());
        assert!(!right.is_left());

        // Test serialization
        assert_eq!(serde_json::to_string(&left).unwrap(), r#""left""#);
        assert_eq!(serde_json::to_string(&right).unwrap(), r#""right""#);
        assert_eq!(
            serde_json::from_str::<Side>(r#""left""#).unwrap(),
            Side::Left
        );
        assert_eq!(
            serde_json::from_str::<Side>(r#""right""#).unwrap(),
            Side::Right
        );
    }

    #[test]
    fn test_anchor() {
        assert_eq!(Anchor::default(), Anchor::TopLeft);

        assert_eq!(Anchor::TopLeft.to_string(), "TopLeft");
        assert_eq!(Anchor::TopCenter.to_string(), "TopCenter");
        assert_eq!(Anchor::TopRight.to_string(), "TopRight");
        assert_eq!(Anchor::BottomLeft.to_string(), "BottomLeft");
        assert_eq!(Anchor::BottomCenter.to_string(), "BottomCenter");
        assert_eq!(Anchor::BottomRight.to_string(), "BottomRight");

        assert_eq!(
            serde_json::to_string(&Anchor::TopLeft).unwrap(),
            r#"top-left"#
        );
        assert_eq!(
            serde_json::to_string(&Anchor::TopCenter).unwrap(),
            r#"top-center"#
        );
        assert_eq!(
            serde_json::to_string(&Anchor::TopRight).unwrap(),
            r#"top-right"#
        );
        assert_eq!(
            serde_json::to_string(&Anchor::BottomLeft).unwrap(),
            r#"bottom-left"#
        );
        assert_eq!(
            serde_json::to_string(&Anchor::BottomCenter).unwrap(),
            r#"bottom-center"#
        );
        assert_eq!(
            serde_json::to_string(&Anchor::BottomRight).unwrap(),
            r#"bottom-right"#
        );

        assert_eq!(
            serde_json::from_str::<Anchor>(r#"top-left"#).unwrap(),
            Anchor::TopLeft
        );
        assert_eq!(
            serde_json::from_str::<Anchor>(r#"top-center"#).unwrap(),
            Anchor::TopCenter
        );
        assert_eq!(
            serde_json::from_str::<Anchor>(r#"top-right"#).unwrap(),
            Anchor::TopRight
        );
        assert_eq!(
            serde_json::from_str::<Anchor>(r#"bottom-left"#).unwrap(),
            Anchor::BottomLeft
        );
        assert_eq!(
            serde_json::from_str::<Anchor>(r#"bottom-center"#).unwrap(),
            Anchor::BottomCenter
        );
        assert_eq!(
            serde_json::from_str::<Anchor>(r#"bottom-right"#).unwrap(),
            Anchor::BottomRight
        );

        assert!(Anchor::TopLeft.is_top());
        assert!(Anchor::TopCenter.is_top());
        assert!(Anchor::TopRight.is_top());
        assert!(!Anchor::BottomLeft.is_top());
        assert!(!Anchor::BottomCenter.is_top());
        assert!(!Anchor::BottomRight.is_top());

        assert!(Anchor::BottomLeft.is_bottom());
        assert!(Anchor::BottomCenter.is_bottom());
        assert!(Anchor::BottomRight.is_bottom());
        assert!(!Anchor::TopLeft.is_bottom());
        assert!(!Anchor::TopCenter.is_bottom());
        assert!(!Anchor::TopRight.is_bottom());

        assert!(Anchor::TopLeft.is_left());
        assert!(Anchor::BottomLeft.is_left());
        assert!(!Anchor::TopCenter.is_left());
        assert!(!Anchor::BottomCenter.is_left());
        assert!(!Anchor::TopRight.is_left());
        assert!(!Anchor::BottomRight.is_left());

        assert!(Anchor::TopRight.is_right());
        assert!(Anchor::BottomRight.is_right());
        assert!(!Anchor::TopLeft.is_right());
        assert!(!Anchor::BottomLeft.is_right());
        assert!(!Anchor::TopCenter.is_right());
        assert!(!Anchor::BottomCenter.is_right());

        assert!(Anchor::TopCenter.is_center());
        assert!(Anchor::BottomCenter.is_center());
        assert!(!Anchor::TopLeft.is_center());
        assert!(!Anchor::TopRight.is_center());
        assert!(!Anchor::BottomLeft.is_center());
        assert!(!Anchor::BottomRight.is_center());
    }

    #[test]
    fn test_anchor_swap_vertical() {
        assert_eq!(Anchor::TopLeft.swap_vertical(), Anchor::BottomLeft);
        assert_eq!(Anchor::TopCenter.swap_vertical(), Anchor::BottomCenter);
        assert_eq!(Anchor::TopRight.swap_vertical(), Anchor::BottomRight);
        assert_eq!(Anchor::BottomLeft.swap_vertical(), Anchor::TopLeft);
        assert_eq!(Anchor::BottomCenter.swap_vertical(), Anchor::TopCenter);
        assert_eq!(Anchor::BottomRight.swap_vertical(), Anchor::TopRight);

        assert_eq!(
            Anchor::TopLeft.swap_vertical().swap_vertical(),
            Anchor::TopLeft
        );
        assert_eq!(
            Anchor::TopCenter.swap_vertical().swap_vertical(),
            Anchor::TopCenter
        );
        assert_eq!(
            Anchor::BottomRight.swap_vertical().swap_vertical(),
            Anchor::BottomRight
        );
    }

    #[test]
    fn test_anchor_swap_horizontal() {
        assert_eq!(Anchor::TopLeft.swap_horizontal(), Anchor::TopRight);
        assert_eq!(Anchor::TopCenter.swap_horizontal(), Anchor::TopCenter);
        assert_eq!(Anchor::TopRight.swap_horizontal(), Anchor::TopLeft);
        assert_eq!(Anchor::BottomLeft.swap_horizontal(), Anchor::BottomRight);
        assert_eq!(Anchor::BottomCenter.swap_horizontal(), Anchor::BottomCenter);
        assert_eq!(Anchor::BottomRight.swap_horizontal(), Anchor::BottomLeft);

        assert_eq!(
            Anchor::TopLeft.swap_horizontal().swap_horizontal(),
            Anchor::TopLeft
        );
        assert_eq!(
            Anchor::BottomRight.swap_horizontal().swap_horizontal(),
            Anchor::BottomRight
        );
        assert_eq!(Anchor::TopCenter.swap_horizontal(), Anchor::TopCenter);
        assert_eq!(Anchor::BottomCenter.swap_horizontal(), Anchor::BottomCenter);
    }

    #[test]
    fn test_anchor_from_corner() {
        use gpui::Corner;

        assert_eq!(Anchor::from(Corner::TopLeft), Anchor::TopLeft);
        assert_eq!(Anchor::from(Corner::TopRight), Anchor::TopRight);
        assert_eq!(Anchor::from(Corner::BottomLeft), Anchor::BottomLeft);
        assert_eq!(Anchor::from(Corner::BottomRight), Anchor::BottomRight);

        let anchor: Anchor = Corner::TopLeft.into();
        assert_eq!(anchor, Anchor::TopLeft);

        let anchor: Anchor = Corner::BottomRight.into();
        assert_eq!(anchor, Anchor::BottomRight);
    }

    #[test]
    fn test_anchor_to_corner() {
        use gpui::Corner;

        assert_eq!(Corner::from(Anchor::TopLeft), Corner::TopLeft);
        assert_eq!(Corner::from(Anchor::TopRight), Corner::TopRight);
        assert_eq!(Corner::from(Anchor::BottomLeft), Corner::BottomLeft);
        assert_eq!(Corner::from(Anchor::BottomRight), Corner::BottomRight);

        assert_eq!(Corner::from(Anchor::TopCenter), Corner::TopLeft);
        assert_eq!(Corner::from(Anchor::BottomCenter), Corner::BottomLeft);

        let corner: Corner = Anchor::TopLeft.into();
        assert_eq!(corner, Corner::TopLeft);

        let corner: Corner = Anchor::TopCenter.into();
        assert_eq!(corner, Corner::TopLeft);

        let corner: Corner = Anchor::BottomRight.into();
        assert_eq!(corner, Corner::BottomRight);
    }
}
