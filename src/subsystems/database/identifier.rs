use serde::Serialize;
use std::borrow::Cow;
use utoipa::ToSchema;

pub trait IdentifierPrefix {
    fn prefix() -> char;
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct UsernameIdentifier;
impl IdentifierPrefix for UsernameIdentifier {
    #[inline(always)]
    fn prefix() -> char {
        'u'
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct DatabaseIdentifier;
impl IdentifierPrefix for DatabaseIdentifier {
    #[inline(always)]
    fn prefix() -> char {
        'd'
    }
}

pub type UserIdentifier = StructuredIdentifier<UsernameIdentifier>;
pub type DbIdentifier = StructuredIdentifier<DatabaseIdentifier>;

// <p><4b>_<1..=23b>
// u11111111_hello
#[derive(ToSchema, Clone, Copy, PartialEq, Eq, Hash)]
#[schema(as = String)]
pub struct StructuredIdentifier<P: IdentifierPrefix> {
    short_uuid: u32,
    label_len: u8,
    label: [u8; 23],
    #[schema(ignore)]
    _marker: std::marker::PhantomData<P>,
}

impl<P: IdentifierPrefix> std::fmt::Display for StructuredIdentifier<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{:08x}_{}", P::prefix(), self.short_uuid, self.label())
    }
}

impl<P: IdentifierPrefix> std::str::FromStr for StructuredIdentifier<P> {
    type Err = Cow<'static, str>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let prefix = P::prefix();
        let Some(trimmed) = s.strip_prefix(prefix) else {
            return Err(format!("identifier must start with '{prefix}'").into());
        };
        let Some((short_uuid, label)) = trimmed.split_once('_') else {
            return Err("invalid identifier format".into());
        };

        if short_uuid.len() != 8 || !short_uuid.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err("invalid short UUID".into());
        }
        if label.len() < 2 {
            return Err("label part too short".into());
        }
        if !label.bytes().all(|b| b.is_ascii_alphanumeric()) {
            return Err("label part must be alphanumeric".into());
        }

        let label_bytes = label.as_bytes();
        let mut label_array = [0; 23];
        let Some(label_array_slice) = label_array.get_mut(..label_bytes.len()) else {
            return Err("label part too long".into());
        };
        label_array_slice.copy_from_slice(label_bytes);

        Ok(Self {
            short_uuid: u32::from_str_radix(short_uuid, 16).map_err(|_| "invalid short UUID")?,
            label_len: label_bytes.len() as u8,
            label: label_array,
            _marker: std::marker::PhantomData,
        })
    }
}

impl<P: IdentifierPrefix> Serialize for StructuredIdentifier<P> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.to_string().serialize(serializer)
    }
}

impl<P: IdentifierPrefix> StructuredIdentifier<P> {
    pub fn from_parts(short_uuid: u32, label: &str) -> Result<Self, anyhow::Error> {
        if label.len() < 2 {
            anyhow::bail!("label part too short");
        }
        if !label.bytes().all(|b| b.is_ascii_alphanumeric()) {
            anyhow::bail!("label part must be alphanumeric");
        }

        let mut label_array = [0; 23];
        let Some(label_array_slice) = label_array.get_mut(..label.len()) else {
            anyhow::bail!("label part too long");
        };
        label_array_slice.copy_from_slice(label.as_bytes());

        Ok(Self {
            short_uuid,
            label_len: label.len() as u8,
            label: label_array,
            _marker: std::marker::PhantomData,
        })
    }

    #[inline]
    pub fn short_uuid(&self) -> u32 {
        self.short_uuid
    }

    #[inline]
    pub fn label(&self) -> &str {
        // SAFETY: The label is always valid UTF-8, and the length is correct.
        unsafe {
            std::str::from_utf8_unchecked(self.label.get_unchecked(..self.label_len as usize))
        }
    }
}
